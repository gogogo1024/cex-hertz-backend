package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type posRow struct {
	ID       int64  `gorm:"column:id"`
	Volume   string `gorm:"column:volume"`
	AvgPrice string `gorm:"column:avg_price"`
}

func ensureCheckpointTable(db *gorm.DB, table string) error {
	// minimal checkpoint table for resume
	return db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (job_name text PRIMARY KEY, last_id bigint NOT NULL DEFAULT 0, updated_at timestamptz DEFAULT now())`, table)).Error
}

func tryAdvisoryLock(db *gorm.DB, key int64) (bool, error) {
	var locked sql.NullBool
	row := db.Raw("SELECT pg_try_advisory_lock(?)", key).Row()
	if err := row.Scan(&locked); err != nil {
		return false, err
	}
	return locked.Valid && locked.Bool, nil
}

func releaseAdvisoryLock(db *gorm.DB, key int64) {
	_ = db.Exec("SELECT pg_advisory_unlock(?)", key).Error
}

func getLastCheckpoint(db *gorm.DB, table, job string) (int64, error) {
	var last sql.NullInt64
	row := db.Raw(fmt.Sprintf("SELECT last_id FROM %s WHERE job_name = ?", table), job).Row()
	if err := row.Scan(&last); err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if !last.Valid {
		return 0, nil
	}
	return last.Int64, nil
}

func upsertCheckpoint(db *gorm.DB, table, job string, lastID int64) error {
	// upsert last processed id
	return db.Exec(fmt.Sprintf(`INSERT INTO %s (job_name,last_id,updated_at) VALUES (?, ?, now()) ON CONFLICT (job_name) DO UPDATE SET last_id = EXCLUDED.last_id, updated_at = now()`, table), job, lastID).Error
}

func main() {
	var dsn string
	var batch int
	var dryRun bool
	var throttleMs int
	var maxRows int
	var checkpointTable string
	var jobName string
	var lockKey int64

	flag.StringVar(&dsn, "dsn", os.Getenv("PG_DSN"), "Postgres DSN. Can also be set via PG_DSN env")
	flag.IntVar(&batch, "batch", 500, "rows per batch")
	flag.BoolVar(&dryRun, "dry-run", true, "dry run (do not write changes)")
	flag.IntVar(&throttleMs, "throttle", 100, "ms sleep between batches")
	flag.IntVar(&maxRows, "max", 0, "max rows to process (0 = all)")
	flag.StringVar(&checkpointTable, "checkpoint-table", "backfill_checkpoints", "checkpoint table name")
	flag.StringVar(&jobName, "job-name", "positions_backfill", "logical job name for checkpointing")
	flag.Int64Var(&lockKey, "lock-key", 123456789, "Postgres advisory lock key to prevent concurrent runs")
	flag.Parse()

	if dsn == "" {
		log.Fatal("dsn is required (use -dsn or set PG_DSN)")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}

	// create checkpoint table if not exists
	if err := ensureCheckpointTable(db, checkpointTable); err != nil {
		log.Fatalf("ensure checkpoint table failed: %v", err)
	}

	// attempt advisory lock to avoid concurrent runs
	locked, err := tryAdvisoryLock(db, lockKey)
	if err != nil {
		log.Fatalf("failed to acquire advisory lock: %v", err)
	}
	if !locked {
		log.Printf("another instance holds the advisory lock (key=%d), exiting", lockKey)
		return
	}
	defer releaseAdvisoryLock(db, lockKey)

	// handle signals to exit gracefully
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	processed := 0
	start := time.Now()

	lastID, err := getLastCheckpoint(db, checkpointTable, jobName)
	if err != nil {
		log.Fatalf("get checkpoint failed: %v", err)
	}

	log.Printf("starting backfill job=%s from id>%d batch=%d dry-run=%v", jobName, lastID, batch, dryRun)

	for {
		select {
		case <-ctx.Done():
			log.Printf("received termination signal, stopping after current batch")
			goto FINISH
		default:
		}

		var rows []posRow
		res := db.Table("positions").Select("id, volume, avg_price").Where("id > ? AND (volume_bigint IS NULL OR avg_price_bigint IS NULL)", lastID).Order("id").Limit(batch).Find(&rows)
		if res.Error != nil {
			log.Fatalf("query failed: %v", res.Error)
		}
		if len(rows) == 0 {
			break
		}

		// process batch inside a transaction
		tx := db.Begin()
		if tx.Error != nil {
			log.Fatalf("failed to begin tx: %v", tx.Error)
		}

		var maxProcessedID int64 = lastID
		for _, r := range rows {
			if maxRows > 0 && processed >= maxRows {
				log.Printf("reached maxRows=%d, stopping", maxRows)
				break
			}

			var vol model.QuantityInNano
			var avg model.PriceInNano

			if r.Volume != "" {
				q, err := model.ParseQuantity(r.Volume)
				if err != nil {
					log.Printf("warning: parse quantity failed id=%d volume=%q: %v", r.ID, r.Volume, err)
				} else {
					vol = q
				}
			}
			if r.AvgPrice != "" {
				p, err := model.ParsePrice(r.AvgPrice)
				if err != nil {
					log.Printf("warning: parse price failed id=%d avg_price=%q: %v", r.ID, r.AvgPrice, err)
				} else {
					avg = p
				}
			}

			if dryRun {
				fmt.Printf("[dry] id=%d -> volume_bigint=%d avg_price_bigint=%d\n", r.ID, int64(vol), int64(avg))
			} else {
				// only update if still NULL to be idempotent
				u := tx.Table("positions").Where("id = ? AND (volume_bigint IS NULL OR avg_price_bigint IS NULL)", r.ID).Updates(map[string]interface{}{
					"volume_bigint":    int64(vol),
					"avg_price_bigint": int64(avg),
				})
				if u.Error != nil {
					log.Printf("update failed id=%d: %v", r.ID, u.Error)
					// record and continue; do not abort whole batch
					continue
				}
				if u.RowsAffected > 0 {
					fmt.Printf("updated id=%d\n", r.ID)
				} else {
					// nothing updated (race or already filled)
					log.Printf("skipped id=%d (already filled)", r.ID)
				}
			}

			processed++
			if r.ID > maxProcessedID {
				maxProcessedID = r.ID
			}
		}

		// commit batch
		if err := tx.Commit().Error; err != nil {
			log.Fatalf("commit failed: %v", err)
		}

		// update checkpoint to maxProcessedID to allow resume
		if !dryRun && maxProcessedID > lastID {
			if err := upsertCheckpoint(db, checkpointTable, jobName, maxProcessedID); err != nil {
				log.Fatalf("checkpoint upsert failed: %v", err)
			}
			lastID = maxProcessedID
		}

		// throttle between batches
		time.Sleep(time.Duration(throttleMs) * time.Millisecond)
	}

FINISH:
	elapsed := time.Since(start)
	log.Printf("backfill finished, processed=%d rows, elapsed=%s", processed, elapsed)
}
