package broker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/shivanisha/crypto-stream-analytics/services/market-data-producer/model"
)

// symbolBalancer assigns one partition per symbol by matching the message
// key against a configurable symbol list.  The position in the list
// determines the partition index: the first symbol gets the first partition,
// the second gets the second, and so on.
//
// This gives a strict 1:1 mapping when symbols == partitions, and a
// deterministic fan-out otherwise.  Unknown symbols (keys not in the list)
// fall back to the first partition.
type symbolBalancer struct {
	symbols []string
}

func (b *symbolBalancer) Balance(msg kafka.Message, partitions ...int) int {
	for i, sym := range b.symbols {
		if string(msg.Key) == sym {
			if i < len(partitions) {
				return partitions[i]
			}
			return partitions[i%len(partitions)]
		}
	}
	// Key we don't recognise — safest to pin to one place.
	return partitions[0]
}

// Writer wraps a Kafka producer and publishes normalized trade events.
type Writer struct {
	inner *kafka.Writer
}

// NewWriter creates a Kafka writer configured for the trade event contract.
//
//   - Each symbol maps to its own partition via a deterministic symbol
//     balancer so downstream consumers can assume 1 partition = 1 symbol.
//   - BatchTimeout keeps latency low (10 ms) while still allowing
//     micro-batching during bursts.
//   - WriteTimeout caps how long Publish blocks on an unresponsive broker
//     so the upstream WebSocket read loop is never stalled indefinitely.
func NewWriter(brokers []string, topic string, symbols []string) *Writer {
	return &Writer{
		inner: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &symbolBalancer{symbols: symbols},
			BatchTimeout:           10 * time.Millisecond,
			WriteTimeout:           5 * time.Second,
		},
	}
}

// Publish serialises event to JSON and sends it to Kafka with symbol as the
// partition key.
func (w *Writer) Publish(ctx context.Context, event model.TradeEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return w.inner.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.Symbol),
		Value: value,
	})
}

// Close flushes any buffered messages and closes the underlying Kafka
// connections.
func (w *Writer) Close() error {
	return w.inner.Close()
}
