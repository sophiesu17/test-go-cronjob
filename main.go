package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-co-op/gocron/v2"
)

func main() {
	processStart := time.Now()
	ctx, cf := context.WithCancel(context.Background())

	s, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}
	defer func() {
		err = s.Shutdown()
		if err != nil {
			panic(err)
		}
	}()

	var wg sync.WaitGroup
	_, err = s.NewJob(
		// run for each second
		gocron.CronJob("* * * * * *", true),
		NewPubSubTask(ctx, processStart, &wg),
	)
	if err != nil {
		panic(err)
	}

	s.Start()
	wg.Add(1)
	go listenTermSignalAndStopDispatcher(cf, &wg)

	wg.Wait()
}

func NewPubSubTask(ctx context.Context, processStart time.Time, wg *sync.WaitGroup) gocron.Task {
	return gocron.NewTask(func(ctx context.Context, wg *sync.WaitGroup) {
		select {
		case <-ctx.Done():
			return // Stop the task
		default:
		}
		wg.Add(1)
		jobStart := time.Now()
		// Use monotonic clock instead of "wall clock", which may be changed by clock synchronization
		// see: https://pkg.go.dev/time#hdr-Monotonic_Clocks
		// Also, drop the fraction part for modding.
		diffInSeconds, _ := math.Modf(jobStart.Sub(processStart).Seconds())

		// Run for each 3 seconds
		mod := math.Mod(diffInSeconds, 3)
		if mod > 0 {
			return
		}
		fmt.Println("Run job for", jobStart)
		// do the long time things
		t := time.Tick(30 * time.Second)
		<-t
		fmt.Println(jobStart.Format("15:04:05"), "Job Done", time.Now().Sub(jobStart))
		wg.Done()
	}, ctx, wg)
}

func listenTermSignalAndStopDispatcher(cf context.CancelFunc, wg *sync.WaitGroup) {
	fmt.Println("Listening for termination signals...")
	termSignal := make(chan os.Signal, 1)
	signal.Notify(termSignal, syscall.SIGTERM, syscall.SIGINT)
	signal := <-termSignal

	fmt.Printf("Received system signal: %s\n", signal.String())
	wg.Done()
	cf()
}
