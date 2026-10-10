// Command transactional shows the reason to keep work in your database: the
// work is created in the same transaction as your own writes, so it exists if
// and only if the transaction commits.
//
//	go run ./transactional
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/tobibamidele/lathe/sqlite"
	"github.com/tobibamidele/validate"
	"github.com/tobibamidele/work"
	"github.com/tobibamidele/work/database"
)

type SendReceipt struct {
	OrderID string `json:"order_id"`
}

var SendReceiptSchema = validate.Object[SendReceipt](validate.Field("order_id", validate.String().Min(1)))

func main() {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "work-tx-")
	defer os.RemoveAll(dir)
	db, err := sqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if _, err := database.Migrate(ctx, db); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE orders (id TEXT PRIMARY KEY, total INTEGER NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	backend, err := database.New(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	registry := work.NewRegistry()
	registry.MustRegister(work.Define("send-receipt", SendReceiptSchema, func(ctx context.Context, r SendReceipt) error {
		fmt.Println("receipt sent for", r.OrderID)
		return nil
	}))
	rt, err := work.NewRuntime(backend, registry, work.RuntimeConfig{})
	if err != nil {
		log.Fatal(err)
	}

	placeOrder := func(id string, total int) error {
		return database.WithTx(ctx, db, rt, func(tx *database.Tx) error {
			if _, err := tx.DB().Exec(ctx, "INSERT INTO orders (id, total) VALUES (?, ?)", id, total); err != nil {
				return err
			}
			if total > 1000 {
				return errors.New("payment declined") // rolls back the order AND the receipt
			}
			return tx.Enqueue(SendReceipt{OrderID: id})
		})
	}

	fmt.Println("place o-1:", placeOrder("o-1", 50))
	fmt.Println("place o-2:", placeOrder("o-2", 5000))

	orders, _ := db.Query(ctx, "SELECT COUNT(*) FROM orders")
	var n int
	orders.Next()
	_ = orders.Scan(&n)
	orders.Close()
	pending, _ := rt.List(ctx, work.ListFilter{})
	fmt.Printf("orders stored: %d, receipts pending: %d (o-2 left neither behind)\n", n, len(pending))

	w, _ := work.NewWorker(backend, registry, work.WorkerConfig{PollInterval: 20 * time.Millisecond})
	runCtx, stop := context.WithTimeout(ctx, 400*time.Millisecond)
	defer stop()
	if err := w.Run(runCtx); err != nil {
		log.Fatal(err)
	}
}
