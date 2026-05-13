package strategy

import (
	"fmt"
	"math"
	"notifier/model"
	"notifier/ta"
)

type EMAGapRSIStrategy struct {
	Symbol          string
	Timeframe       string
	EmaFastLen      int
	EmaSlowLen      int
	GapPercent      float64
	PriceGapPercent float64
	RSILen          int
}

// Constructor
func NewEMAGapRSIStrategy(
	symbol, timeframe string,
	emaFastLen, emaSlowLen int,
	gapPercent, priceGapPercent float64,
	rsiLen int,
) *EMAGapRSIStrategy {
	return &EMAGapRSIStrategy{
		Symbol:          symbol,
		Timeframe:       timeframe,
		EmaFastLen:      emaFastLen,
		EmaSlowLen:      emaSlowLen,
		GapPercent:      gapPercent,
		PriceGapPercent: priceGapPercent,
		RSILen:          rsiLen,
	}
}

func (s *EMAGapRSIStrategy) Name() string {
	return "EMA Gap + RSI Strategy " + s.Symbol + " " + s.Timeframe
}

func (s *EMAGapRSIStrategy) Process(candles []model.Candle) Signal {
	minCandles := max(s.EmaSlowLen, s.RSILen)
	if len(candles) < minCandles {
		return Hold
	}

	closes := model.GetCloses(candles)

	// Indicators
	emaFast := ta.EMA(closes, s.EmaFastLen)
	emaSlow := ta.EMA(closes, s.EmaSlowLen)
	rsiValues := ta.RSI(closes, s.RSILen)

	i := len(closes) - 1

	// Latest values
	closePrice := closes[i]
	emaFastVal := emaFast[i]
	emaSlowVal := emaSlow[i]

	rsi := rsiValues[i]
	prevRSI := rsiValues[i-1]

	if isNaN(rsi) || isNaN(prevRSI) {
		return Hold
	}

	// ───── RSI Cross ─────
	rsiCrossUp := prevRSI < 30 && rsi > 30
	rsiCrossDown := prevRSI > 70 && rsi < 70

	// ───── EMA Gap ─────
	emaGapPercent := math.Abs(emaFastVal-emaSlowVal) / emaSlowVal * 100
	significantGap := emaGapPercent >= s.GapPercent

	// ───── Price ↔ EMA Fast Gap ─────
	priceGapPercent := math.Abs(closePrice-emaFastVal) / emaFastVal * 100
	priceFar := priceGapPercent >= s.PriceGapPercent

	fmt.Printf("EMA50: %.2f EMA200: %.2f RSI: %.2f Price: %.2f\n", emaFastVal, emaSlowVal, rsi, closePrice)
	// ───── Conditions ─────
	sellSignal :=
		emaFastVal > emaSlowVal &&
			significantGap &&
			priceFar &&
			rsiCrossDown

	buySignal :=
		emaFastVal < emaSlowVal &&
			significantGap &&
			priceFar &&
			rsiCrossUp

	if buySignal {
		return Buy
	}

	if sellSignal {
		return Sell
	}

	return Hold
}

func (s *EMAGapRSIStrategy) GetSymbol() string {
	return s.Symbol
}

func (s *EMAGapRSIStrategy) GetTimeframe() string {
	return s.Timeframe
}

func (s *EMAGapRSIStrategy) GetMinRequiredCandles() int {
	return max(s.EmaSlowLen, s.RSILen) + 2
}
