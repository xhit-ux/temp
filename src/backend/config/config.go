package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds all application configuration loaded from environment variables.
// No hardcoded defaults for critical values — all must be provided via .env.
type Config struct {
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBDatabase string

	ServerPort int

	// Batch writer settings
	BatchSize    int // max events per batch (triggers flush)
	FlushSeconds int // max seconds to wait before flushing a partial batch

	// Queue settings
	QueueCapacity         int // max events in normal memory channel
	PriorityQueueCapacity int // max events in priority memory channel

	// Retry settings
	MaxRetries     int
	RetryBaseDelay int // seconds

	// Dead letter settings
	DeadLetterDir              string
	DeadLetterReplayInterval   int // seconds between replay scans
	DeadLetterReplayBatchSize  int // max events per replay batch

	// Writer pool
	WriterPoolSize int
}

// Load reads .env from project root and returns a validated Config.
func Load(envPath string) (*Config, error) {
	if envPath != "" {
		if err := godotenv.Load(envPath); err != nil {
			return nil, fmt.Errorf("failed to load .env file %s: %w", envPath, err)
		}
	} else {
		// Load from project root (src/backend → ../../ = project root)
		_ = godotenv.Load("../../.env")
		_ = godotenv.Load(".env")
	}

	cfg := &Config{}

	// Required database settings
	cfg.DBHost = requireEnv("DB_HOST")
	cfg.DBUser = requireEnv("DB_USERNAME")
	cfg.DBPassword = requireEnv("DB_PASSWORD")
	cfg.DBDatabase = requireEnv("DB_DATABASE")

	var err error
	cfg.DBPort, err = requireIntEnv("DB_PORT")
	if err != nil {
		return nil, err
	}

	// Server port
	cfg.ServerPort, err = optionalIntEnv("SERVER_PORT", 4048)
	if err != nil {
		return nil, err
	}

	// Batch writer settings
	cfg.BatchSize, err = optionalIntEnv("BATCH_SIZE", 500)
	if err != nil {
		return nil, err
	}
	cfg.FlushSeconds, err = optionalIntEnv("FLUSH_SECONDS", 5)
	if err != nil {
		return nil, err
	}

	// Queue capacity
	cfg.QueueCapacity, err = optionalIntEnv("QUEUE_CAPACITY", 10000)
	if err != nil {
		return nil, err
	}
	cfg.PriorityQueueCapacity, err = optionalIntEnv("PRIORITY_QUEUE_CAPACITY", 2000)
	if err != nil {
		return nil, err
	}

	// Retry settings
	cfg.MaxRetries, err = optionalIntEnv("MAX_RETRIES", 5)
	if err != nil {
		return nil, err
	}
	cfg.RetryBaseDelay, err = optionalIntEnv("RETRY_BASE_DELAY_SECONDS", 1)
	if err != nil {
		return nil, err
	}

	// Writer pool
	cfg.WriterPoolSize, err = optionalIntEnv("WRITER_POOL_SIZE", 2)
	if err != nil {
		return nil, err
	}

	// Dead letter settings
	cfg.DeadLetterDir = os.Getenv("DEAD_LETTER_DIR")
	if cfg.DeadLetterDir == "" {
		cfg.DeadLetterDir = "data/dead-letter"
	}
	cfg.DeadLetterReplayInterval, err = optionalIntEnv("DEAD_LETTER_REPLAY_INTERVAL_SECONDS", 30)
	if err != nil {
		return nil, err
	}
	cfg.DeadLetterReplayBatchSize, err = optionalIntEnv("DEAD_LETTER_REPLAY_BATCH_SIZE", 100)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// DSN returns the PostgreSQL connection string.
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		c.DBHost, c.DBPort, c.DBUser, c.DBPassword, c.DBDatabase,
	)
}

// HTTPAddress returns the server listen address.
func (c *Config) HTTPAddress() string {
	return fmt.Sprintf(":%d", c.ServerPort)
}

func requireEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		panic(fmt.Sprintf("required environment variable %s is not set", key))
	}
	return val
}

func requireIntEnv(key string) (int, error) {
	val := os.Getenv(key)
	if val == "" {
		return 0, fmt.Errorf("required environment variable %s is not set", key)
	}
	return strconv.Atoi(val)
}

func optionalIntEnv(key string, defaultVal int) (int, error) {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal, nil
	}
	return strconv.Atoi(val)
}