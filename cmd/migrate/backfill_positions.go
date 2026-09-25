package main

import (
	"flag"
	"fmt"
	"log"
	"os"
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

func main() {
	var dsn string
	var batch int
	var dryRun bool
	var throttleMs int
	var maxRows int

	flag.StringVar(&dsn, "dsn", os.Getenv("PG_DSN"), "Postgres DSN. Can also be set via PG_DSN env")
	flag.IntVar(&batch, "batch", 500, "rows per batch")
	flag.BoolVar(&dryRun, "dry-run", true, "dry run (do not write changes)")
	flag.IntVar(&throttleMs, "throttle", 100, "ms sleep between batches")
	flag.IntVar(&maxRows, "max", 0, "max rows to process (0 = all)")
	flag.Parse()

	if dsn == "" {
		log.Fatal("dsn is required (use -dsn or set PG_DSN)")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}

	processed := 0
	for {
		var rows []posRow
		res := db.Table("positions").Select("id, volume, avg_price").Where("volume_bigint IS NULL OR avg_price_bigint IS NULL").Order("id").Limit(batch).Find(&rows)
		if res.Error != nil {
			log.Fatalf("query failed: %v", res.Error)
		}
		if len(rows) == 0 {
			break
		}

		for _, r := range rows {
			if maxRows > 0 && processed >= maxRows {
				log.Printf("reached maxRows=%d, stopping", maxRows)
				goto DONE
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
				u := db.Table("positions").Where("id = ?", r.ID).Updates(map[string]interface{}{
					"volume_bigint":    int64(vol),
					"avg_price_bigint": int64(avg),
				})
				if u.Error != nil {
					log.Printf("update failed id=%d: %v", r.ID, u.Error)
					continue
				}
				fmt.Printf("updated id=%d\n", r.ID)
			}

			processed++
		}

		time.Sleep(time.Duration(throttleMs) * time.Millisecond)
	}

DONE:
	fmt.Printf("backfill finished, processed=%d rows\n", processed)
}
