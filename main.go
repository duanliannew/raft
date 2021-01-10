package main

import (
	"fmt"
	"context"
	"time"
	"sync"
	log "github.com/sirupsen/logrus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	var wait sync.WaitGroup

	wait.Add(1)

	go worker0(ctx, &wait)

	time.Sleep(1 * time.Second)

	cancel()
	
	wait.Wait()

	fmt.Println("main exits")

	log.Info("Something noteworthy happened!")
}

func worker0(ctx context.Context, wait *sync.WaitGroup) {
	ctx_n, cancel := context.WithTimeout(ctx, 2 * time.Second)
	defer cancel()

	go func() {
		worker1(ctx_n)
	}()

	select {
	case <-ctx.Done():
		fmt.Println("worker0", ctx.Err())
	case <-time.After(3 * time.Second):
		fmt.Println("worker0 completes its work")
	}
	wait.Done()
}

func worker1(ctx context.Context) {
	select {
	case <-ctx.Done():
		fmt.Println("worker1", ctx.Err())
	case <-time.After(4 * time.Second):
		fmt.Println("worker1 completes its work")
	}
}