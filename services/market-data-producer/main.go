package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/shivanisha/crypto-stream-analytics/services/market-data-producer/binance"
	"github.com/shivanisha/crypto-stream-analytics/services/market-data-producer/broker"
	"github.com/shivanisha/crypto-stream-analytics/services/market-data-producer/config"
	"github.com/shivanisha/crypto-stream-analytics/services/market-data-producer/model"
)

func main() {
	_ = godotenv.Load()

	cfg := config.Load()

	writer := broker.NewWriter(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.Symbols)
	defer writer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("starting market-data-producer (symbols: %s)", strings.Join(cfg.Symbols, ", "))

	handler := func(event model.TradeEvent) {
		if err := writer.Publish(ctx, event); err != nil {
			log.Printf("publish error: %v", err)
		}
		log.Printf("%s price=%f qty=%f", event.Symbol, event.Price, event.Quantity)
	}

	client := binance.NewClient(cfg.Symbols, cfg.ReconnectDelay)
	if err := client.Stream(ctx, handler); err != nil {
		log.Printf("stream exited: %v", err)
	}

	log.Println("shutdown complete")
}
