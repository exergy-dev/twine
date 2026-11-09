// Command server is the twine echo server for the 1M idle-connection soak
// (design Phase 6). It listens on -ports consecutive ports so that one
// process can hold 1M connections within the 1,048,576 fd limit and the
// per-listener accept queues are spread, raises RLIMIT_NOFILE to the hard
// limit, and prints RSS, HeapInuse and the Connections metric every -interval.
//
//	go run ./idle1m/server -loops 8 -ports 40 -base 20000 -pin 0,2,4,6,8,10,12,14
//
// Drive it with ../ (bench/idle1m).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/exergy-dev/twine"
	"github.com/exergy-dev/twine/bench"
)

// echo is the same protocol object as the bench echo server (an 16 B struct
// registered as a streaming producer), so per-connection memory is comparable
// with the EchoIdle benchmark.
type echo struct{ t twine.Transport }

func (e *echo) ConnectionMade(t twine.Transport) { e.t = t; t.RegisterProducer(e, true) }
func (e *echo) DataReceived(b []byte)            { e.t.Write(b) }
func (e *echo) ConnectionLost(error)             {}
func (e *echo) PauseProducing()                  { e.t.PauseReading() }
func (e *echo) ResumeProducing()                 { e.t.ResumeReading() }
func (e *echo) StopProducing()                   {}

func main() {
	host := flag.String("host", "127.0.0.1", "listen host")
	base := flag.Int("base", 20000, "first port (keep outside ip_local_port_range 32768-60999)")
	ports := flag.Int("ports", 40, "number of consecutive ports to listen on")
	loops := flag.Int("loops", 8, "event loops")
	pin := flag.String("pin", "0,2,4,6,8,10,12,14", "PinLoops CPU list; empty = no pinning")
	backlog := flag.Int("backlog", 65535, "listen backlog (capped by net.core.somaxconn)")
	interval := flag.Duration("interval", 10*time.Second, "stats print interval")
	dur := flag.Duration("duration", 0, "exit after this long (0 = until SIGINT)")
	flag.Parse()

	lim, err := bench.RaiseNoFile()
	if err != nil {
		log.Fatal(err)
	}
	opts := []twine.Option{twine.Loops(*loops), twine.Watchdog(0), twine.ShutdownTimeout(5 * time.Second)}
	if *pin != "" {
		cpus, err := bench.ParseCPUs(*pin)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, twine.PinLoops(cpus...))
	}
	eng := twine.New(opts...)
	f := twine.FactoryFunc(func(net.Addr) twine.Protocol { return &echo{} })
	for i := 0; i < *ports; i++ {
		if _, err := eng.ListenTCP(fmt.Sprintf("%s:%d", *host, *base+i), f, twine.Backlog(*backlog)); err != nil {
			log.Fatal(err)
		}
	}
	runErr := make(chan error, 1)
	go func() { runErr <- eng.Run(context.Background()) }()
	for _, l := range eng.Loops() {
		done := make(chan struct{})
		l.CallFromThread(func() { close(done) })
		select {
		case <-done:
		case err := <-runErr:
			log.Fatal(err)
		}
	}
	fmt.Printf("# server pid=%d loops=%d ports=%d..%d nofile=%d pin=%q GOMAXPROCS=%d\n",
		os.Getpid(), *loops, *base, *base+*ports-1, lim, *pin, runtime.GOMAXPROCS(0))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	var end <-chan time.Time
	if *dur > 0 {
		end = time.After(*dur)
	}
	tk := time.NewTicker(*interval)
	t0 := time.Now()
	for {
		select {
		case <-tk.C:
			var conns uint64
			for _, l := range eng.Loops() {
				conns += l.Metrics().Connections
			}
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			fmt.Printf("server t=%4.0fs conns=%d rss_mb=%.1f heap_inuse_mb=%.1f heap_sys_mb=%.1f stack_inuse_mb=%.1f num_gc=%d\n",
				time.Since(t0).Seconds(), conns, float64(bench.ProcessRSS())/1e6, float64(ms.HeapInuse)/1e6,
				float64(ms.HeapSys)/1e6, float64(ms.StackInuse)/1e6, ms.NumGC)
		case <-sig:
			eng.Stop()
			<-eng.Done()
			return
		case <-end:
			eng.Stop()
			<-eng.Done()
			return
		case err := <-runErr:
			log.Fatal(err)
		}
	}
}
