package model

// BinanceTrade mirrors the raw trade fields Binance sends inside the
// combined-stream "data" envelope.  Price and quantity arrive as strings
// to avoid floating-point loss during JSON transport.
type BinanceTrade struct {
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
}

// BinanceEnvelope is the wrapper Binance uses for combined-stream messages
// (wss://stream.binance.com:9443/stream?streams=...).
type BinanceEnvelope struct {
	Stream string       `json:"stream"`
	Data   BinanceTrade `json:"data"`
}

// TradeEvent is the normalized contract defined in AGENT.md.  Every
// consumer downstream (Flink, future services) expects this shape.
type TradeEvent struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Quantity  float64 `json:"quantity"`
	TradeTime int64   `json:"trade_time"`
}
