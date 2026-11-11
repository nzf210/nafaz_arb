// Command nafaz-scanner-outbox inspects and repairs the scanner's durable
// Supabase outbox journal.
//
// It exists for one failure mode: rows the scanner could build but PostgREST
// will never accept (PGRST102 duplicate idempotency_key). Those records are
// replayed oldest-first, so a single poisoned record stops every later scan
// from reaching scanner_opportunities. Deleting the whole file also throws away
// the healthy backlog, so prune by the recorded PostgREST error instead.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/nafaz/spot-perp-scanner/internal/supabase"
)

const defaultOutboxPath = "/app/data/scanner/supabase-outbox.jsonl"

// defaultPattern matches permanent schema rejections only. Widening this to a
// bare status code would also drop transient 5xx/429 backlog that would
// otherwise deliver once the outage ends.
const defaultPattern = `PGRST102|All object keys must match|ON CONFLICT DO UPDATE command cannot affect row a second time`

func main() {
	path := flag.String("path", envOr("SUPABASE_SCANNER_OUTBOX_PATH", defaultOutboxPath), "outbox JSONL path")
	pattern := flag.String("pattern", defaultPattern, "regexp matched against each record's last delivery error")
	minAttempts := flag.Int("min-attempts", 3, "only prune records that failed at least this many times")
	dryRun := flag.Bool("dry-run", true, "report what would be pruned without touching the journal")
	list := flag.Bool("list", false, "print every pending record with its attempt count and last error, then exit")
	apply := flag.Bool("apply", false, "actually rewrite the journal (moves the original to <path>.bak)")
	flag.Parse()

	if *apply {
		*dryRun = false
	}
	if _, err := os.Stat(*path); err != nil {
		fatal("outbox not found at %s: %v", *path, err)
	}
	outbox, err := supabase.NewDurableOutbox(*path, 0)
	if err != nil {
		fatal("open outbox: %v", err)
	}

	if *list {
		records, err := outbox.Inspect()
		if err != nil {
			fatal("%v", err)
		}
		for _, record := range records {
			fmt.Printf("scan=%-32s attempts=%-3d rows=%-3d next=%s err=%s\n",
				record.ScanID, record.Attempts, record.Rows,
				record.NextAttemptAt.Format("2006-01-02T15:04:05Z07:00"), record.LastError)
		}
		fmt.Printf("pending=%d\n", len(records))
		return
	}

	report, err := outbox.PruneUndeliverable(*pattern, *minAttempts, *dryRun)
	if err != nil {
		fatal("%v", err)
	}
	fmt.Printf("path=%s\nrecords=%d kept=%d dropped=%d dropped_rows=%d\n",
		*path, report.Total, report.Kept, report.Dropped, report.DroppedRows)
	if report.SampleScanID != "" {
		fmt.Printf("sample_dropped_scan=%s\n", report.SampleScanID)
	}
	reasons := make([]string, 0, len(report.Reasons))
	for reason := range report.Reasons {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		fmt.Printf("reason[%d]=%s\n", report.Reasons[reason], reason)
	}
	switch {
	case report.Dropped == 0:
		fmt.Println("nothing matched; journal unchanged")
	case report.Truncated:
		fmt.Println("WARNING: drop cap reached; re-run with the same flags after verifying")
	case *dryRun:
		fmt.Println("dry run; re-run with -apply to rewrite the journal")
	default:
		fmt.Printf("journal rewritten; previous file kept at %s.bak\n", *path)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "nafaz-scanner-outbox: "+format+"\n", args...)
	os.Exit(1)
}
