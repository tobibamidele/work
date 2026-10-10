// Package cli provides the operator commands for durable work, meant to be
// mounted inside your application's own command line:
//
//	myapp work inspect <id>
//	myapp work failed
//	myapp work retry <id>
//	myapp work cancel <id>
//
// The package builds on the public Runtime inspection API only, so it works
// with every backend and adds no behavior of its own. Wire it up like this:
//
//	case "work":
//	    err = cli.Run(ctx, rt, os.Args[2:], os.Stdout)
//
// Authentication and authorization are the application's responsibility:
// these commands can read payloads and change the state of work.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/tobibamidele/work"
)

// Usage is the help text.
const Usage = `usage: work <command> [arguments]

commands:
  inspect <id> [--payload]       show one work item (payloads are hidden unless --payload)
  list [--status s] [--queue q] [--type t] [--limit n]
                                 list work, newest first
  failed [--queue q] [--type t] [--limit n]
                                 list failed work
  retry <id>                     run failed or cancelled work again (resets attempts)
  cancel <id>                    cancel work (running work is asked to stop)
  stats                          counts per queue and status
`

// ErrUsage marks a command line the user got wrong. The usage text has
// already been printed.
var ErrUsage = errors.New("work: usage error")

// Run executes one command, writing results to out. args excludes the program
// and the "work" word: for `myapp work retry abc` pass ["retry", "abc"].
func Run(ctx context.Context, rt *work.Runtime, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(out, Usage)
		if len(args) == 0 {
			return ErrUsage
		}
		return nil
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "inspect":
		return inspect(ctx, rt, rest, out)
	case "list":
		return list(ctx, rt, rest, out, false)
	case "failed":
		return list(ctx, rt, rest, out, true)
	case "retry":
		id, err := oneID(cmd, rest, out)
		if err != nil {
			return err
		}
		if err := rt.Retry(ctx, id); err != nil {
			return describe(err, id)
		}
		fmt.Fprintf(out, "work %s is available again\n", id)
		return nil
	case "cancel":
		id, err := oneID(cmd, rest, out)
		if err != nil {
			return err
		}
		outcome, err := rt.Cancel(ctx, id)
		if err != nil {
			return describe(err, id)
		}
		switch outcome {
		case work.Cancelled:
			fmt.Fprintf(out, "work %s cancelled\n", id)
		default:
			fmt.Fprintf(out, "work %s is running: its handler was asked to stop and will be settled by its worker\n", id)
		}
		return nil
	case "stats":
		return stats(ctx, rt, out)
	}
	fmt.Fprintf(out, "unknown command %q\n\n%s", cmd, Usage)
	return ErrUsage
}

func oneID(cmd string, rest []string, out io.Writer) (work.ID, error) {
	if len(rest) != 1 || strings.HasPrefix(rest[0], "-") {
		fmt.Fprintf(out, "usage: work %s <id>\n", cmd)
		return "", ErrUsage
	}
	return work.ID(rest[0]), nil
}

// describe turns backend sentinels into messages an operator can act on.
func describe(err error, id work.ID) error {
	switch {
	case errors.Is(err, work.ErrNotFound):
		return fmt.Errorf("work %s does not exist", id)
	case errors.Is(err, work.ErrAlreadyCompleted):
		return fmt.Errorf("work %s already completed: nothing to do", id)
	case errors.Is(err, work.ErrInvalidState):
		return fmt.Errorf("%w (only failed or cancelled work can be retried; only unfinished work can be cancelled)", err)
	case errors.Is(err, work.ErrDuplicate):
		return fmt.Errorf("another active work item holds the same unique key: %w", err)
	}
	return err
}

func inspect(ctx context.Context, rt *work.Runtime, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	fs.SetOutput(out)
	payload := fs.Bool("payload", false, "print the payload (may contain personal data)")
	id, rest := splitID(args)
	if err := fs.Parse(rest); err != nil || id == "" || fs.NArg() != 0 {
		fmt.Fprintln(out, "usage: work inspect <id> [--payload]")
		return ErrUsage
	}
	rec, err := rt.Get(ctx, work.ID(id))
	if err != nil {
		return describe(err, work.ID(id))
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	row := func(k string, v any) { fmt.Fprintf(w, "%s\t%v\n", k, v) }
	row("id", rec.ID)
	row("type", fmt.Sprintf("%s (v%d)", rec.Type, rec.Version))
	row("queue", rec.Queue)
	row("status", rec.Status)
	row("attempts", fmt.Sprintf("%d of %d", rec.Attempts, rec.MaxAttempts))
	row("priority", rec.Priority)
	row("available at", ts(rec.AvailableAt))
	row("created at", ts(rec.CreatedAt))
	row("updated at", ts(rec.UpdatedAt))
	if !rec.StartedAt.IsZero() {
		row("last started", ts(rec.StartedAt))
	}
	if !rec.CompletedAt.IsZero() {
		row("completed at", ts(rec.CompletedAt))
	}
	if rec.WorkerID != "" {
		row("worker", rec.WorkerID)
		row("lease until", ts(rec.LeaseUntil))
	}
	if !rec.CancelRequestedAt.IsZero() {
		row("cancel requested", ts(rec.CancelRequestedAt))
	}
	if rec.UniqueKey != "" {
		row("unique key", rec.UniqueKey)
	}
	if rec.IdempotencyKey != "" {
		row("idempotency key", rec.IdempotencyKey)
	}
	if rec.Timeout > 0 {
		row("timeout", rec.Timeout)
	}
	if rec.LastError != "" {
		row("last error at", ts(rec.LastErrorAt))
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if rec.LastError != "" {
		fmt.Fprintf(out, "\nlast error:\n%s\n", rec.LastError)
	}
	if *payload {
		fmt.Fprintf(out, "\npayload:\n%s\n", rec.Payload)
	}
	return nil
}

// splitID lets the id come before or after the flags.
func splitID(args []string) (string, []string) {
	for i, a := range args {
		if !strings.HasPrefix(a, "-") {
			return a, append(append([]string(nil), args[:i]...), args[i+1:]...)
		}
	}
	return "", args
}

func list(ctx context.Context, rt *work.Runtime, args []string, out io.Writer, failedOnly bool) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(out)
	status := fs.String("status", "", "filter by status")
	queue := fs.String("queue", "", "filter by queue")
	typ := fs.String("type", "", "filter by work type")
	limit := fs.Int("limit", 50, "maximum number of rows")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *limit < 1 {
		return ErrUsage
	}
	f := work.ListFilter{Queue: *queue, Type: *typ, Limit: *limit}
	switch {
	case failedOnly:
		f.Statuses = []work.Status{work.StatusFailed}
	case *status != "":
		s := work.Status(*status)
		if !s.Valid() {
			return fmt.Errorf("unknown status %q (one of %v)", *status, work.AllStatuses)
		}
		f.Statuses = []work.Status{s}
	}
	recs, err := rt.List(ctx, f)
	if err != nil {
		return err
	}
	if len(recs) == 0 {
		fmt.Fprintln(out, "no work found")
		return nil
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tTYPE\tQUEUE\tSTATUS\tATTEMPTS\tCREATED\tLAST ERROR")
	for _, r := range recs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d/%d\t%s\t%s\n", r.ID, r.Type, r.Queue, r.Status, r.Attempts, r.MaxAttempts, ts(r.CreatedAt), oneLine(r.LastError, 60))
	}
	return w.Flush()
}

func stats(ctx context.Context, rt *work.Runtime, out io.Writer) error {
	st, err := rt.Stats(ctx)
	if err != nil {
		return err
	}
	if len(st) == 0 {
		fmt.Fprintln(out, "no work found")
		return nil
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "QUEUE\tSTATUS\tCOUNT")
	for _, s := range st {
		fmt.Fprintf(w, "%s\t%s\t%d\n", s.Queue, s.Status, s.Count)
	}
	return w.Flush()
}

func ts(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format("2006-01-02 15:04:05Z")
}

func oneLine(s string, max int) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > max {
		s = s[:max-1] + "…"
	}
	return s
}
