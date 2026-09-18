package main

import (
	"context"
	"fmt"
	"time"
)

type failoverConfig struct {
	seedAddr string
	peers    map[int]string
	duration time.Duration
	interval time.Duration // delay between probes; bounds measurement resolution
}

type probeRecord struct {
	t  time.Time
	ok bool
}

// runFailover continuously PUTs a fresh key while the caller manually
// kills (or `docker compose kill`/`stop`s) the current leader in another
// terminal, then reports the resulting client-visible unavailability
// window — the "recovering from leader failure in [x] ms" number the
// design doc's resume target (§23) calls for.
func runFailover(cfg failoverConfig) {
	client := newKVClient(cfg.seedAddr, cfg.peers)
	defer client.Close()

	fmt.Println("=== Failover Benchmark ===")
	fmt.Printf("Probing with PUT every %s for %s — kill the current leader now.\n", cfg.interval, cfg.duration)

	deadline := time.Now().Add(cfg.duration)
	var records []probeRecord
	n := 0
	for time.Now().Before(deadline) {
		n++
		key := fmt.Sprintf("failover-probe-%d", n)
		ctx, cancel := context.WithTimeout(context.Background(), rpcAttemptTimeout)
		err := client.Put(ctx, key, "x")
		cancel()
		records = append(records, probeRecord{t: time.Now(), ok: err == nil})
		time.Sleep(cfg.interval)
	}

	printFailoverReport(records)
}

func printFailoverReport(records []probeRecord) {
	total := len(records)
	var okCount int
	for _, r := range records {
		if r.ok {
			okCount++
		}
	}
	fmt.Printf("attempts: %d (ok=%d err=%d)\n", total, okCount, total-okCount)

	minDur, maxDur, failedProbes, found := longestOutage(records)
	if !found {
		fmt.Println("longest outage: none detected (no leader failure occurred during the run)")
		return
	}
	fmt.Printf("longest outage: between %s and %s (%d consecutive failed probes)\n",
		minDur.Round(time.Millisecond), maxDur.Round(time.Millisecond), failedProbes)
	fmt.Println("(min = span between the first and last failed probe; max = span between the last ok probe before and the first ok probe after — true recovery time lies in between, bounded by the probe interval)")
}

// longestOutage finds the longest consecutive run of failed probes and
// returns a [min, max] bound on how long the cluster was actually
// unavailable: min is measured between the first and last failed probe in
// that run, max between the last ok probe before it and the first ok
// probe after it (when the run doesn't touch either edge of the test
// window, in which case max falls back to min).
func longestOutage(records []probeRecord) (minDur, maxDur time.Duration, runLen int, found bool) {
	var bestLen int
	var bestFirstFail, bestLastFail, bestOkBefore, bestOkAfter time.Time

	i := 0
	for i < len(records) {
		if records[i].ok {
			i++
			continue
		}
		start := i
		for i < len(records) && !records[i].ok {
			i++
		}
		end := i - 1 // inclusive

		if length := end - start + 1; length > bestLen {
			bestLen = length
			bestFirstFail = records[start].t
			bestLastFail = records[end].t
			if start > 0 {
				bestOkBefore = records[start-1].t
			} else {
				bestOkBefore = time.Time{}
			}
			if end+1 < len(records) {
				bestOkAfter = records[end+1].t
			} else {
				bestOkAfter = time.Time{}
			}
		}
	}

	if bestLen == 0 {
		return 0, 0, 0, false
	}
	minDur = bestLastFail.Sub(bestFirstFail)
	if !bestOkBefore.IsZero() && !bestOkAfter.IsZero() {
		maxDur = bestOkAfter.Sub(bestOkBefore)
	} else {
		maxDur = minDur
	}
	return minDur, maxDur, bestLen, true
}
