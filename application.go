package raft_etcd_benchmark

import (
	"context"
	"fmt"
	"time"

	pb "github.com/coreos/etcd/raft/raftpb"
)

type Application struct {
	lastTerm  uint64
	lastIndex uint64
	data      []byte
}

func NewApplication() *Application {
	return &Application{
		lastTerm:  0,
		lastIndex: 0,
		data:      []byte{},
	}
}

func (a *Application) ApplyEntries(ctx context.Context, entries ...pb.Entry) {
	for _, e := range entries {
		if e.Type != pb.EntryNormal {
			panic("unexpected msg type: " + e.Type.String())
		}
		if e.Term < a.lastTerm {
			panic(fmt.Sprintf("somethings wrong, lastTerm: %d, e.Term: %d", a.lastTerm, e.Term))
		}
		if e.Index <= a.lastIndex {
			panic(fmt.Sprintf("somethings wrong, lastIndex: %d, e.Index: %d", a.lastIndex, e.Index))
		}
		a.lastTerm = e.Term
		a.lastIndex = e.Index

		if len(e.Data) == 0 {
			continue
		}
		if len(a.data) == 0 {
			a.data = append(a.data, e.Data[0])
			continue
		}
		// 保障顺序
		//if e.Data[0] <= a.data[len(a.data)-1] && a.data[len(a.data)-1] != byte(255) && e.Data[0] != 0 {
		//	panic(fmt.Sprintf("somethings wrong, data: %d, e.Data: %d, len: %d", a.data[len(a.data)-1], e.Data, len(a.data)))
		//}
		a.data = append(a.data, e.Data[0])
	}
	// 模拟一下时延
	time.Sleep(time.Duration(len(entries)/3) * time.Millisecond)
}
