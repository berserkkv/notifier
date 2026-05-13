package strategy

import "notifier/model"

// Signal represents a trading signal
type Signal int

const (
	Hold Signal = iota
	Buy
	Sell
)

func (s Signal) String() string {
	switch s {
	case Buy:
		return "BUY"
	case Sell:
		return "SELL"
	default:
		return "HOLD"
	}
}

// Strategy interface for all trading strategies
type Strategy interface {
	// Name returns the strategy name
	Name() string
	// Process analyzes candles and returns a signal
	Process(candles []model.Candle) Signal
	// Symbol returns the symbol for this strategy
	GetSymbol() string
	// Timeframe returns the timeframe for this strategy
	GetTimeframe() string
	// GetMinRequiredCandles returns the minimum number of candles required for this strategy
	GetMinRequiredCandles() int
}

func isNaN(f float64) bool {
	return f != f
}
