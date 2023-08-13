package raft_etcd_benchmark

import (
	"context"
	"time"

	pb "github.com/coreos/etcd/raft/raftpb"
)

var net *Net = &Net{
	delay: 2 * time.Millisecond,
	m:     map[uint64]*Process{},
}

type Net struct {
	delay time.Duration
	m     map[uint64]*Process
}

func (n *Net) Send(to uint64, message pb.Message) error {
	time.Sleep(n.delay)
	//if rand.Intn(2) == 0 {
	//	return errors.New("rand drop message")
	//}
	n.m[to].RecvRaftRPC(context.Background(), message)
	return nil
}
