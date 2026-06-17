package config

import (
	"os"
	"strings"
	"time"
)

// Config holds all runtime configuration sourced from environment variables.
// Every field has a sensible default so the service starts without any
// environment setup.
type Config struct {
	KafkaBrokers   []string
	KafkaTopic     string
	Symbols        []string
	ReconnectDelay time.Duration
}

// Load reads configuration from environment variables, falling back to
// defaults when a variable is absent or empty.
func Load() Config {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	symbols := strings.Split(getEnv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"), ",")
	for i := range symbols {
		symbols[i] = strings.TrimSpace(symbols[i])
	}

	return Config{
		KafkaBrokers:   brokers,
		KafkaTopic:     getEnv("KAFKA_TOPIC", "crypto_trades"),
		Symbols:        symbols,
		ReconnectDelay: time.Duration(3) * time.Second,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
