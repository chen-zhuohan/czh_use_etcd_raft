package raft_etcd_benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
	_ "unsafe"
)

func Benchmark(b *testing.B) {
	var n uint64 = 3

	for i := uint64(0); i < n; i++ {
		net.m[i+1] = New((i)%n+1, (i+1)%n+1, (i+2)%n+1)
	}
	for _, p := range net.m {
		p.Run(context.Background())
	}

	time.Sleep(2 * time.Second)

	var p *Process
	for _, pp := range net.m {
		if pp.IsLeader() {
			p = pp
		}
	}
	if p == nil {
		panic("no leader")
	}
	//bs := randBytes(1024)

	b.Run("post", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f := StartMSTimer()
			//p = net.m[uint64(i%3)+1]
			err := p.Propose(context.Background(), []byte{byte(i % 256)})
			if err != nil {
				panic(err)
			}

			b.ReportMetric(float64(f()), "ns")
		}
	})
	fmt.Println("msg count", atomic.LoadUint64(&p.msgCount))
}

func randBytes(n int) []byte {
	r := make([]byte, n)
	rand.Read(r)
	return r
}
