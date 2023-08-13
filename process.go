package raft_etcd_benchmark

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe"

	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type Process struct {
	Ticker time.Duration

	id          uint64
	node        raft.Node
	storage     *MemoryStorage
	application *Application
	msgCount    uint64
}

func New(peers ...uint64) *Process {
	c := &raft.Config{
		ID:                        peers[0],
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   NewMemoryStorage(),
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	rpeers := make([]raft.Peer, len(peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: peers[i]}
	}
	node := raft.StartNode(c, rpeers)

	return &Process{
		Ticker:      10 * time.Millisecond,
		id:          peers[0],
		node:        node,
		storage:     c.Storage.(*MemoryStorage),
		application: NewApplication(),
	}
}

func (p *Process) Propose(ctx context.Context, data []byte) error {
	return p.node.Propose(ctx, data)
}

func (p *Process) RecvRaftRPC(ctx context.Context, m pb.Message) error {
	// 唯一返回err的可能就是返回了ctx.Error，所以忽略。
	return p.node.Step(ctx, m)
}

func (p *Process) Run(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(p.Ticker)

		for {
			select {
			case <-ticker.C:
				p.node.Tick()
			case rd := <-p.node.Ready():
				if !raft.IsEmptySnap(rd.Snapshot) {
					panic("not handle snapshot")
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					p.storage.SetHardState(rd.HardState)
				}
				p.storage.Append(rd.Entries)

				f := StartMSTimer()
				wg := sync.WaitGroup{}
				wg.Add(len(rd.Messages))
				for _, msg := range rd.Messages {
					atomic.AddUint64(&p.msgCount, 1)
					go func(msg pb.Message) {
						defer wg.Done()
						if err := net.Send(msg.To, msg); err != nil {
							p.node.ReportUnreachable(msg.To)
							//panic(err)
						}
						if msg.Type == pb.MsgSnap {
							panic("why reach here")
							//a.node.ReportSnapshot(msg.To, raft.SnapshotFinish)
						}
					}(msg)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					for _, e := range rd.CommittedEntries {
						if e.Type == pb.EntryConfChange {
							var cc pb.ConfChange
							cc.Unmarshal(e.Data)
							p.node.ApplyConfChange(cc)
						} else if e.Type == pb.EntryNormal {
							p.application.ApplyEntries(ctx, e)
						} else {
							panic("unknown type: " + e.Type.String())
						}
					}
				}()
				wg.Wait()
				p.node.Advance()

				if len(rd.Messages) > 0 {
					fmt.Println(f(), "\t", len(rd.CommittedEntries), "\t", len(rd.Messages[0].Entries))
				} else {
					fmt.Println(f(), "\t", len(rd.CommittedEntries))
				}
			}
		}
	}()
}

func (p *Process) IsLeader() bool {
	//fmt.Println(p.node.Status().RaftState, p.node.Status().Lead, p.node.Status().Term, p.node.Status().Vote)
	return p.node.Status().RaftState == raft.StateLeader
}

//go:linkname runtimeNow runtime.nanotime
func runtimeNow() int64

func StartMSTimer() func() time.Duration {
	now := runtimeNow()
	return func() time.Duration {
		return time.Duration(runtimeNow() - now)
	}
}
