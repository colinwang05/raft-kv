package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

type throughputConfig struct {
	seedAddr  string
	peers     map[int]string
	workers   int
	duration  time.Duration
	readPct   int // 0-100: percentage of ops that are GET instead of PUT
	valueSize int
	keySpace  int // number of distinct keys cycled through
}

type opResult struct {
	latency time.Duration
	failed  bool
}

func runThroughput(cfg throughputConfig) {
	value := strings.Repeat("x", cfg.valueSize)
	deadline := time.Now().Add(cfg.duration)

	var mu sync.Mutex
	var results []opResult
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := newKVClient(cfg.seedAddr, cfg.peers)
			defer client.Close()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(workerID)))
			var local []opResult
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("bench-key-%d", rng.Intn(cfg.keySpace))

				opStart := time.Now()
				var err error
				if cfg.readPct > 0 && rng.Intn(100) < cfg.readPct {
					_, _, err = client.Get(context.Background(), key)
				} else {
					err = client.Put(context.Background(), key, value)
				}
				local = append(local, opResult{latency: time.Since(opStart), failed: err != nil})
			}

			mu.Lock()
			results = append(results, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	printThroughputReport(cfg, elapsed, results)
}

func printThroughputReport(cfg throughputConfig, elapsed time.Duration, results []opResult) {
	total := len(results)
	var errCount int
	latencies := make([]time.Duration, 0, total)
	for _, r := range results {
		if r.failed {
			errCount++
			continue
		}
		latencies = append(latencies, r.latency)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Println("=== Throughput Benchmark ===")
	fmt.Printf("workers=%d duration=%s read-pct=%d%% key-space=%d value-size=%dB\n",
		cfg.workers, cfg.duration, cfg.readPct, cfg.keySpace, cfg.valueSize)
	fmt.Printf("total ops:  %d (ok=%d err=%d)\n", total, total-errCount, errCount)
	fmt.Printf("elapsed:    %s\n", elapsed.Round(time.Millisecond))
	if elapsed > 0 {
		fmt.Printf("throughput: %.1f ops/sec\n", float64(total)/elapsed.Seconds())
	}
	if len(latencies) > 0 {
		round := time.Microsecond * 100
		fmt.Printf("latency:    p50=%s  p95=%s  p99=%s  max=%s\n",
			percentile(latencies, 50).Round(round),
			percentile(latencies, 95).Round(round),
			percentile(latencies, 99).Round(round),
			latencies[len(latencies)-1].Round(round))
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
