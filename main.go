package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/time/rate"
)

type job struct {
	id int64
}

func isSelect(q string) bool {
	trim := strings.TrimSpace(q)
	if trim == "" {
		return false
	}
	// check first word
	first := strings.ToLower(strings.Fields(trim)[0])
	return first == "select" || first == "with" // CTEs
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	pos := (p / 100.0) * float64(len(sorted)-1)
	i := int(pos)
	f := pos - float64(i)
	if i+1 < len(sorted) {
		return sorted[i] + f*(sorted[i+1]-sorted[i])
	}
	return sorted[i]
}

func main() {
	var (
		dsn         string
		driver      string
		rps         int
		concurrency int
		duration    time.Duration
		timeout     time.Duration
		warmup      time.Duration
		sqlFile     string
	)

	flag.StringVar(&driver, "driver", "pgx", "database/sql driver (e.g. pgx, postgres, mysql, sqlserver, oracle)")
	flag.StringVar(&dsn, "dsn", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", "database DSN")
	flag.IntVar(&rps, "rps", 150, "target requests per second")
	flag.IntVar(&concurrency, "concurrency", 32, "number of concurrent workers")
	flag.DurationVar(&duration, "duration", 300*time.Second, "test duration")
	flag.DurationVar(&timeout, "timeout", 2*time.Second, "per-request timeout")
	flag.DurationVar(&warmup, "warmup", 2*time.Second, "warm-up period (excluded from stats)")
	flag.StringVar(&sqlFile, "sql-file", "target-sql.txt", "path to the SQL file")
	flag.Parse()

	queryBytes, err := os.ReadFile(sqlFile)
	if err != nil {
		log.Fatalf("read sql file: %v", err)
	}
	query := string(queryBytes)

	log.Printf("Starting SQL load: driver=%s rps=%d concurrency=%d duration=%s timeout=%s",
		driver, rps, concurrency, duration, timeout)

	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// reasonable pool sizing
	db.SetMaxOpenConns(concurrency * 2)
	db.SetMaxIdleConns(concurrency)
	db.SetConnMaxLifetime(10 * time.Minute)

	// Prepare once (safer/faster)
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	// Graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	jobs := make(chan job, rps*2)

	var (
		successes int64
		failures  int64
		reqID     int64
	)

	var latMu sync.Mutex
	latencies := make([]float64, 0, rps*int(duration.Seconds()+5))

	// Worker pool
	wg := &sync.WaitGroup{}
	runQuery := isSelect(query)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range jobs {
				reqCtx, cancel := context.WithTimeout(ctx, timeout)
				start := time.Now()
				var qerr error

				if runQuery {
					rows, err := stmt.QueryContext(reqCtx)
					if err != nil {
						qerr = err
					} else {
						// Drain rows quickly without scanning
						for rows.Next() {
							// noop
						}
						if err := rows.Err(); err != nil {
							qerr = err
						}
						_ = rows.Close()
					}
				} else {
					_, err := stmt.ExecContext(reqCtx)
					if err != nil {
						qerr = err
					}
				}

				elapsed := time.Since(start)
				cancel()

				// Exclude warmup from stats but keep success/fail counts
				if time.Since(testStart) > warmup {
					latMu.Lock()
					latencies = append(latencies, float64(elapsed.Milliseconds()))
					latMu.Unlock()
				}

				if qerr != nil {
					atomic.AddInt64(&failures, 1)
				} else {
					atomic.AddInt64(&successes, 1)
				}

				_ = j // keep for potential per-request logging
			}
		}(w)
	}

	// Rate generator
	lim := rate.NewLimiter(rate.Limit(rps), rps) // burst=rps
	testEnd := time.Now().Add(duration)
	testTicker := time.NewTicker(100 * time.Millisecond)
	defer testTicker.Stop()

	// Capture test start so workers can compute warmup
	testStart = time.Now()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-testTicker.C:
			// generate enough tokens for next 100ms slice
			now := time.Now()
			if now.After(testEnd) {
				break loop
			}
			targetUntil := now.Add(100 * time.Millisecond)
			for now.Before(targetUntil) {
				if err := lim.Wait(ctx); err != nil {
					break loop
				}
				id := atomic.AddInt64(&reqID, 1)
				select {
				case jobs <- job{id: id}:
				case <-ctx.Done():
					break loop
				}
				now = time.Now()
			}
		}
	}

	close(jobs)
	wg.Wait()

	total := successes + failures
	elapsed := time.Since(testStart)
	achievedRPS := float64(total) / elapsed.Seconds()

	latMu.Lock()
	sort.Float64s(latencies)
	p50 := percentile(latencies, 50)
	p90 := percentile(latencies, 90)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	min := math.NaN()
	max := math.NaN()
	if len(latencies) > 0 {
		min = latencies[0]
		max = latencies[len(latencies)-1]
	}
	latMu.Unlock()

	fmt.Println("==== Results ====")
	fmt.Printf("Duration:        %s (warmup excluded from latency stats: %s)\n", elapsed.Truncate(time.Millisecond), warmup)
	fmt.Printf("Target RPS:      %d\n", rps)
	fmt.Printf("Achieved RPS:    %.1f\n", achievedRPS)
	fmt.Printf("Successes:       %d\n", successes)
	fmt.Printf("Failures:        %d\n", failures)
	fmt.Printf("Latencies (ms):  min=%.1f p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n",
		min, p50, p90, p95, p99, max)
}

var testStart time.Time
