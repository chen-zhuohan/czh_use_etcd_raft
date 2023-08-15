package raft_etcd_benchmark

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	_ "unsafe"

	"github.com/panjf2000/ants/v2"
)

func BenchmarkA(b *testing.B) {
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

	b.Run("post", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			//p = net.m[uint64(i%3)+1]
			err := p.Propose(context.Background(), []byte{byte(i % 256)})
			if err != nil {
				panic(err)
			}
		}
	})
	fmt.Println("msg count", atomic.LoadUint64(&p.msgCount))
}

func Benchmark_WithWait(b *testing.B) {
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

	var id uint64
	b.Run("post", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			data := []byte{byte(i % 256)}
			data = binary.AppendUvarint(data, atomic.AddUint64(&id, 1))
			err := p.ProposeWait(context.Background(), data)
			if err != nil {
				panic(err)
			}
		}
	})
	fmt.Println("msg count", atomic.LoadUint64(&p.msgCount))
}

func Benchmark_WithWait_Pally(b *testing.B) {
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

	var id uint64
	b.Run("post", func(b *testing.B) {
		pool, err := ants.NewPool(2500)
		if err != nil {
			panic(err)
		}
		wg := sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			data := []byte{byte(i % 256)}
			data = binary.AppendUvarint(data, atomic.AddUint64(&id, 1))
			err = pool.Submit(func() {
				defer wg.Done()
				err := p.ProposeWait(context.Background(), data)
				if err != nil {
					panic(err)
				}
			})
			if err != nil {
				panic(err)
			}
		}
		wg.Wait()
	})
	fmt.Println("msg count", atomic.LoadUint64(&p.msgCount))
}
