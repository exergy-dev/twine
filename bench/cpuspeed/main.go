// cpuspeed pins N OS threads to distinct vCPUs and reports each one's
// iterations/s of a fixed integer loop, to reveal heterogeneous host cores.
package main

import (
	"flag"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

func main() {
	n := flag.Int("n", 8, "busy threads")
	stride := flag.Int("stride", 2, "cpu stride")
	offset := flag.Int("offset", 0, "first cpu")
	dur := flag.Duration("d", 2*time.Second, "duration")
	flag.Parse()
	rates := make([]float64, *n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runtime.LockOSThread()
			var set unix.CPUSet
			set.Set(*offset + i**stride)
			_ = unix.SchedSetaffinity(0, &set)
			<-start
			deadline := time.Now().Add(*dur)
			var x uint64 = 1
			var iters uint64
			for time.Now().Before(deadline) {
				for k := 0; k < 1000000; k++ {
					x = x*6364136223846793005 + 1442695040888963407
				}
				iters++
			}
			if x == 0 {
				fmt.Print("")
			}
			rates[i] = float64(iters) / dur.Seconds()
		}(i)
	}
	time.Sleep(100 * time.Millisecond)
	close(start)
	wg.Wait()
	sorted := append([]float64(nil), rates...)
	sort.Float64s(sorted)
	fmt.Printf("n=%d stride=%d  per-thread Mloops/s (sorted): ", *n, *stride)
	for _, r := range sorted {
		fmt.Printf("%.0f ", r)
	}
	fmt.Printf("\n  min=%.0f median=%.0f max=%.0f  max/min=%.2f\n", sorted[0], sorted[len(sorted)/2], sorted[len(sorted)-1], sorted[len(sorted)-1]/sorted[0])
}
