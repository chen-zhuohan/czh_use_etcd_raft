package raft_etcd_benchmark

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe"

	pb "github.com/coreos/etcd/raft/raftpb"
	"go.etcd.io/etcd/raft"
)

type Process struct {
	Ticker time.Duration

	id          uint64
	node        raft.Node
	storage     *MemoryStorage
	application *Application
	wait        Wait
	msgCount    uint64
}

func New(peers ...uint64) *Process {
	c := &raft.Config{
		ID:              peers[0],
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         NewMemoryStorage(),
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
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
		wait:        NewWait(),
	}
}

func (p *Process) Propose(ctx context.Context, data []byte) error {
	return p.node.Propose(ctx, data)
}

func (p *Process) ProposeWait(ctx context.Context, data []byte) error {
	id, _ := binary.Uvarint(data[1:])
	ch := p.wait.Register(id)
	err := p.node.Propose(ctx, data)
	if err != nil {
		return err
	}
	<-ch
	return nil
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
				// 1. 处理快照、HardState、Entries
				if !raft.IsEmptySnap(rd.Snapshot) {
					panic("not handle snapshot")
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					p.storage.SetHardState(rd.HardState)
				}
				p.storage.Append(rd.Entries)

				// 2. 发送Message
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

				// 3. 应用 CommittedEntries
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
							if len(e.Data) > 1 {
								id, _ := binary.Uvarint(e.Data[1:])
								p.wait.Trigger(id, id)
							}
						} else {
							panic("unknown type: " + e.Type.String())
						}
					}
				}()
				wg.Wait()
				// 4. 调用Advance
				p.node.Advance()

				//if len(rd.Messages) > 0 {
				//	fmt.Println(f(), "\t", len(rd.CommittedEntries), "\t", len(rd.Messages[0].Entries))
				//} else {
				//	fmt.Println(f(), "\t", len(rd.CommittedEntries))
				//}
			}
		}
	}()
}

func (p *Process) IsLeader() bool {
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
