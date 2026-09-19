package pg

import (
	"context"
	"fmt"

	"github.com/gogogo1024/cex-hertz-backend/biz/model"
	"github.com/gogogo1024/cex-hertz-backend/conf"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var PostgresClient *pgxpool.Pool
var GormDB *gorm.DB

func Init() {
	fmt.Printf("conf: %+v\n", conf.GetConf())
	// 初始化 Postgres 连接池
	pgConf := conf.GetConf().Postgres
	pool, err := pgxpool.New(context.Background(), pgConf.DSN)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to postgres: %v", err))
	}
	if err := pool.Ping(context.Background()); err != nil {
		panic(fmt.Sprintf("failed to ping postgres: %v", err))
	}
	PostgresClient = pool

	// 初始化 GORM DB
	if err := InitGorm(); err != nil {
		panic(fmt.Sprintf("failed to init gorm: %v", err))
	}
	// 自动迁移表结构
	if err := AutoMigrate(); err != nil {
		panic(fmt.Sprintf("failed to auto migrate: %v", err))
	}
}

func InitGorm() error {
	pgConf := conf.GetConf().Postgres
	dsn := pgConf.DSN
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return err
	}
	GormDB = db
	return nil
}

func AutoMigrate() error {
	if GormDB == nil {
		return gorm.ErrInvalidDB
	}
	return GormDB.AutoMigrate(
		&model.Order{},
		&model.Trade{},
		&model.EventOffsetCheckpoint{}, // Event Sourcing V2: Crash recovery checkpoint表
	)
}
func GetPool() *pgxpool.Pool {
	if PostgresClient == nil {
		panic("PostgresClient未初始化，请先调用 pg.Init() 或 pg.InitPostgresPool()")
	}
	return PostgresClient
}
func InitTradeTable() error {
	if GormDB == nil {
		return gorm.ErrInvalidDB
	}
	return GormDB.AutoMigrate(&model.Trade{})
}
