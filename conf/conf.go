// package conf
//
// import (
//
//	"fmt"
//	"os"
//
// )
//
//	type Config struct {
//		PostgresConn  string
//		KafkaBrokers  []string
//		KafkaTopic    string
//		WebSocketAddr string
//		ConsulAddr    string
//		NodeID        string
//		Matchsymbols    []string
//		MatchPort     int
//	}
//
//	func Load() *Config {
//		cfg := &Config{
//			PostgresConn:  getEnv("CEX_POSTGRES_CONN", "postgres://postgres:postgres@localhost:5432/cex?sslmode=disable"),
//			KafkaBrokers:  []string{getEnv("CEX_KAFKA_BROKER", "localhost:9092")},
//			KafkaTopic:    getEnv("CEX_KAFKA_TOPIC", "cex_trades"),
//			WebSocketAddr: getEnv("CEX_WS_ADDR", ":8080"),
//			ConsulAddr:    getEnv("CEX_CONSUL_ADDR", "127.0.0.1:8500"),
//			NodeID:        getEnv("CEX_NODE_ID", "node-1"),
//			Matchsymbols:    parsesymbols(getEnv("CEX_MATCH_symbolS", "BTCUSDT,ETHUSDT")),
//			MatchPort:     getEnvInt("CEX_MATCH_PORT", 9000),
//		}
//		return cfg
//	}
//
//	func parsesymbols(s string) []string {
//		var res []string
//		for _, p := range splitAndTrim(s, ",") {
//			if p != "" {
//				res = append(res, p)
//			}
//		}
//		return res
//	}
//
//	func splitAndTrim(s, sep string) []string {
//		var res []string
//		for _, v := range split(s, sep) {
//			res = append(res, trim(v))
//		}
//		return res
//	}
//
//	func split(s, sep string) []string {
//		return []string{os.ExpandEnv(s)}
//	}
//
//	func trim(s string) string {
//		return os.ExpandEnv(s)
//	}
//
//	func getEnv(key, def string) string {
//		if v := os.Getenv(key); v != "" {
//			return v
//		}
//		return def
//	}
//
//	func getEnvInt(key string, def int) int {
//		if v := os.Getenv(key); v != "" {
//			var i int
//			_, err := fmt.Sscanf(v, "%d", &i)
//			if err == nil {
//				return i
//			}
//		}
//		return def
//	}
package conf

import (
	"fmt"
	"os"
	"sync"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/kr/pretty"
	"github.com/spf13/viper"
)

var (
	conf *Config
	once sync.Once
)

type Config struct {
	Env         string      `mapstructure:"env" yaml:"env"`
	Hertz       Hertz       `mapstructure:"hertz" yaml:"hertz"`
	MySQL       MySQL       `mapstructure:"mysql" yaml:"mysql"`
	Redis       Redis       `mapstructure:"redis" yaml:"redis"`
	Postgres    Postgres    `mapstructure:"postgres" yaml:"postgres"`
	Kafka       Kafka       `mapstructure:"kafka" yaml:"kafka"`
	MatchEngine MatchEngine `mapstructure:"match_engine" yaml:"match_engine"`
	Registry    Registry    `mapstructure:"registry" yaml:"registry"`
	Recovery    Recovery    `mapstructure:"recovery" yaml:"recovery"`
}

type MySQL struct {
	DSN string `mapstructure:"dsn" yaml:"dsn"`
}

type Redis struct {
	Address  string `mapstructure:"address" yaml:"address"`
	Password string `mapstructure:"password" yaml:"password"`
	Username string `mapstructure:"username" yaml:"username"`
	DB       int    `mapstructure:"db" yaml:"db"`
}
type Postgres struct {
	DSN string `mapstructure:"dsn" yaml:"dsn"`
}
type Kafka struct {
	Brokers []string          `mapstructure:"brokers" yaml:"brokers"`
	Topics  map[string]string `mapstructure:"topics" yaml:"topics"`
}
type Registry struct {
	RegistryAddress []string `mapstructure:"registry_address" yaml:"registry_address"`
	Username        string   `mapstructure:"username" yaml:"username"`
	Password        string   `mapstructure:"password" yaml:"password"`
}
type MatchEngine struct {
	NodeID       string `mapstructure:"node_id" yaml:"node_id"`
	Matchsymbols string `mapstructure:"match_symbols" yaml:"match_symbols"`
	MatchPort    int    `mapstructure:"match_port" yaml:"match_port"`
}
type Hertz struct {
	Service         string `mapstructure:"service" yaml:"service"`
	Address         string `mapstructure:"address" yaml:"address"`
	EnablePprof     bool   `mapstructure:"enable_pprof" yaml:"enable_pprof"`
	EnableGzip      bool   `mapstructure:"enable_gzip" yaml:"enable_gzip"`
	EnableAccessLog bool   `mapstructure:"enable_access_log" yaml:"enable_access_log"`
	LogLevel        string `mapstructure:"log_level" yaml:"log_level"`
	LogFileName     string `mapstructure:"log_file_name" yaml:"log_file_name"`
	LogMaxSize      int    `mapstructure:"log_max_size" yaml:"log_max_size"`
	LogMaxBackups   int    `mapstructure:"log_max_backups" yaml:"log_max_backups"`
	LogMaxAge       int    `mapstructure:"log_max_age" yaml:"log_max_age"`
	RegistryAddr    string `mapstructure:"registry_addr" yaml:"registry_addr"`
	MetricsPort     string `mapstructure:"metrics_port" yaml:"metrics_port"`
	WsPort          string `mapstructure:"ws_port" yaml:"ws_port"`
}

// Recovery 恢复配置
type Recovery struct {
	EnableAutoRecovery   bool   `mapstructure:"enable_auto_recovery" yaml:"enable_auto_recovery"`
	RecoveryTimeout      int    `mapstructure:"recovery_timeout_seconds" yaml:"recovery_timeout_seconds"`
	Strategy             string `mapstructure:"strategy" yaml:"strategy"`
	ValidateAfterRecovery bool  `mapstructure:"validate_after_recovery" yaml:"validate_after_recovery"`
	CheckpointRetentionDays int `mapstructure:"checkpoint_retention_days" yaml:"checkpoint_retention_days"`
	MaxRetries           int    `mapstructure:"max_retries" yaml:"max_retries"`
}

// GetConf gets configuration instance
func GetConf() *Config {
	once.Do(initConf)
	return conf
}

func initConf() {
	v := viper.New()
	// 优先读取 GO_ENV 环境变量
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "test"
	}
	v.SetConfigName("conf") // 不带扩展名
	v.SetConfigType("yaml")
	v.AutomaticEnv()

	// 支持多路径查找，兼容 IDE/workspace 和不同工作目录
	paths := []string{
		fmt.Sprintf("conf/%s", env),       // conf/test/ (项目根目录)
		fmt.Sprintf("../conf/%s", env),    // ../conf/test/ (biz 子目录)
		fmt.Sprintf("../../conf/%s", env), // ../../conf/test/ (biz/service 子目录)
	}
	for _, path := range paths {
		v.AddConfigPath(path)
	}

	if err := v.ReadInConfig(); err != nil {
		panic(fmt.Sprintf("读取配置文件失败: %v", err))
	}
	var c Config
	if err := v.Unmarshal(&c); err != nil {
		panic(fmt.Sprintf("配置文件解析失败: %v", err))
	}
	c.Env = env
	conf = &c
	pretty.Printf("%+v\n", conf)
}

func LogLevel() hlog.Level {
	level := GetConf().Hertz.LogLevel
	switch level {
	case "trace":
		return hlog.LevelTrace
	case "debug":
		return hlog.LevelDebug
	case "info":
		return hlog.LevelInfo
	case "notice":
		return hlog.LevelNotice
	case "warn":
		return hlog.LevelWarn
	case "error":
		return hlog.LevelError
	case "fatal":
		return hlog.LevelFatal
	default:
		return hlog.LevelInfo
	}
}
