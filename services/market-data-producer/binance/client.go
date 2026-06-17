package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/shivanisha/crypto-stream-analytics/services/market-data-producer/model"
)

// Handler is called for each normalized trade event received from Binance.
type Handler func(event model.TradeEvent)

// Client manages a WebSocket connection to Binance's trade stream, handles
// reconnection, and publishes normalized events to the supplied Handler.
type Client struct {
	symbols        []string
	reconnectDelay time.Duration
}

// NewClient creates a Binance stream client for the given symbols.
func NewClient(symbols []string, reconnectDelay time.Duration) *Client {
	return &Client{
		symbols:        symbols,
		reconnectDelay: reconnectDelay,
	}
}

// Stream connects to the Binance combined trade stream and blocks until ctx
// is cancelled.  Disconnections are retried automatically with the
// configured reconnect delay.
func (c *Client) Stream(ctx context.Context, handler Handler) error {
	wsURL := c.streamURL()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("binance dial error: %v — retrying in %v", err, c.reconnectDelay)
			c.sleep(ctx, c.reconnectDelay)
			continue
		}

		go func() {
			<-ctx.Done()
			conn.Close()
		}()

		log.Printf("binance: connected (%s)", strings.Join(c.symbols, ", "))
		c.readLoop(ctx, conn, handler)
		conn.Close()

		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("binance: disconnected — reconnecting in %v", c.reconnectDelay)
		c.sleep(ctx, c.reconnectDelay)
	}
}

func (c *Client) streamURL() string {
	streams := make([]string, len(c.symbols))
	for i, s := range c.symbols {
		streams[i] = fmt.Sprintf("%s@trade", strings.ToLower(s))
	}
	return fmt.Sprintf("wss://stream.binance.com:9443/stream?streams=%s",
		strings.Join(streams, "/"))
}

func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn, handler Handler) {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			log.Printf("binance read error: %v", err)
			return
		}

		event, err := parseTrade(raw)
		if err != nil {
			log.Printf("binance parse error: %v", err)
			continue
		}

		handler(event)
	}
}

func parseTrade(raw []byte) (model.TradeEvent, error) {
	var envelope model.BinanceEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return model.TradeEvent{}, fmt.Errorf("unmarshal envelope: %w", err)
	}

	price, err := strconv.ParseFloat(envelope.Data.Price, 64)
	if err != nil {
		return model.TradeEvent{}, fmt.Errorf("parse price: %w", err)
	}

	qty, err := strconv.ParseFloat(envelope.Data.Quantity, 64)
	if err != nil {
		return model.TradeEvent{}, fmt.Errorf("parse quantity: %w", err)
	}

	return model.TradeEvent{
		Symbol:    envelope.Data.Symbol,
		Price:     price,
		Quantity:  qty,
		TradeTime: envelope.Data.TradeTime / 1000,
	}, nil
}

func (c *Client) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
