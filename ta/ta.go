package ta

import (
	"math"
	"notifier/model"
)

func SMA(data []float64, period int) []float64 {
	result := make([]float64, len(data))

	sum := 0.0
	count := 0

	for i := range data {
		if !math.IsNaN(data[i]) {
			sum += data[i]
			count++
		}

		if i >= period {
			if !math.IsNaN(data[i-period]) {
				sum -= data[i-period]
				count--
			}
		}

		if i >= period-1 && count == period {
			result[i] = sum / float64(period)
		} else {
			result[i] = math.NaN()
		}
	}

	return result
}

func EMA(data []float64, period int) []float64 {
	result := make([]float64, len(data))
	multiplier := 2.0 / float64(period+1)

	// Find first valid (non-NaN) window for SMA seed
	sum := 0.0
	count := 0
	start := -1

	for i := range data {
		if !math.IsNaN(data[i]) {
			sum += data[i]
			count++
		}

		if i >= period {
			if !math.IsNaN(data[i-period]) {
				sum -= data[i-period]
				count--
			}
		}

		if i >= period-1 && count == period {
			start = i
			result[i] = sum / float64(period)
			break
		}
	}

	// If no valid start, return all NaN
	if start == -1 {
		for i := range result {
			result[i] = math.NaN()
		}
		return result
	}

	// Continue EMA
	for i := start + 1; i < len(data); i++ {
		if math.IsNaN(data[i]) || math.IsNaN(result[i-1]) {
			result[i] = math.NaN()
			continue
		}
		result[i] = (data[i]-result[i-1])*multiplier + result[i-1]
	}

	// Fill before start with NaN
	for i := 0; i < start; i++ {
		result[i] = math.NaN()
	}

	return result
}

// RSI — Wilder smoothing
func RSI(data []float64, period int) []float64 {
	result := make([]float64, len(data))

	var avgGain, avgLoss float64

	for i := 0; i < len(data); i++ {
		if i == 0 {
			result[i] = math.NaN()
			continue
		}

		change := data[i] - data[i-1]
		gain := math.Max(change, 0)
		loss := math.Max(-change, 0)

		if i < period {
			avgGain += gain
			avgLoss += loss
			result[i] = math.NaN()
			continue
		}

		if i == period {
			avgGain /= float64(period)
			avgLoss /= float64(period)
		} else {
			avgGain = (avgGain*float64(period-1) + gain) / float64(period)
			avgLoss = (avgLoss*float64(period-1) + loss) / float64(period)
		}

		if avgLoss == 0 {
			result[i] = 100
		} else {
			rs := avgGain / avgLoss
			result[i] = 100 - (100 / (1 + rs))
		}
	}

	return result
}

// MACD — safe NaN handling
func MACD(data []float64, fastPeriod, slowPeriod, signalPeriod int) ([]float64, []float64, []float64) {
	emaFast := EMA(data, fastPeriod)
	emaSlow := EMA(data, slowPeriod)

	macd := make([]float64, len(data))

	for i := range data {
		if math.IsNaN(emaFast[i]) || math.IsNaN(emaSlow[i]) {
			macd[i] = math.NaN()
		} else {
			macd[i] = emaFast[i] - emaSlow[i]
		}
	}

	signal := EMA(macd, signalPeriod)

	hist := make([]float64, len(data))
	for i := range data {
		if math.IsNaN(macd[i]) || math.IsNaN(signal[i]) {
			hist[i] = math.NaN()
		} else {
			hist[i] = macd[i] - signal[i]
		}
	}

	return macd, signal, hist
}

// Bollinger Bands — unchanged logic, cleaned
func BollingerBands(data []float64, period int, stdDev float64) ([]float64, []float64, []float64) {
	sma := SMA(data, period)

	upper := make([]float64, len(data))
	middle := make([]float64, len(data))
	lower := make([]float64, len(data))

	for i := range data {
		middle[i] = sma[i]

		if i < period-1 || math.IsNaN(sma[i]) {
			upper[i] = math.NaN()
			lower[i] = math.NaN()
			continue
		}

		var variance float64
		for j := 0; j < period; j++ {
			diff := data[i-j] - sma[i]
			variance += diff * diff
		}

		std := math.Sqrt(variance / float64(period))

		upper[i] = sma[i] + stdDev*std
		lower[i] = sma[i] - stdDev*std
	}

	return upper, middle, lower
}

// ATR — unchanged (already correct)
func ATR(highs, lows, closes []float64, period int) []float64 {
	result := make([]float64, len(closes))
	tr := make([]float64, len(closes))

	tr[0] = highs[0] - lows[0]

	for i := 1; i < len(closes); i++ {
		tr1 := highs[i] - lows[i]
		tr2 := math.Abs(highs[i] - closes[i-1])
		tr3 := math.Abs(lows[i] - closes[i-1])
		tr[i] = math.Max(tr1, math.Max(tr2, tr3))
	}

	for i := range closes {
		if i < period-1 {
			result[i] = math.NaN()
			continue
		}

		if i == period-1 {
			sum := 0.0
			for j := 0; j < period; j++ {
				sum += tr[i-j]
			}
			result[i] = sum / float64(period)
		} else {
			result[i] = (result[i-1]*float64(period-1) + tr[i]) / float64(period)
		}
	}

	return result
}

// Stochastic — fixed loop
func Stochastic(candles []model.Candle, kPeriod, dPeriod int) ([]float64, []float64) {

	highs := make([]float64, len(candles))
	lows := make([]float64, len(candles))
	closes := make([]float64, len(candles))

	for i, candle := range candles {
		highs[i] = candle.High
		lows[i] = candle.Low
		closes[i] = candle.Close
	}

	k := make([]float64, len(closes))

	for i := range closes {
		if i < kPeriod-1 {
			k[i] = math.NaN()
			continue
		}

		highestHigh := highs[i]
		lowestLow := lows[i]

		for j := 1; j < kPeriod; j++ {
			if highs[i-j] > highestHigh {
				highestHigh = highs[i-j]
			}
			if lows[i-j] < lowestLow {
				lowestLow = lows[i-j]
			}
		}

		if highestHigh == lowestLow {
			k[i] = 50
		} else {
			k[i] = (closes[i] - lowestLow) / (highestHigh - lowestLow) * 100
		}
	}

	d := SMA(k, dPeriod)

	return k, d
}

// Heikin Ashi — unchanged (already correct)
func HeikinAshi(candles []model.Candle) []model.Candle {
	haCandles := make([]model.Candle, len(candles))

	var prevHAOpen, prevHAClose float64

	for i, c := range candles {
		haClose := (c.Open + c.High + c.Low + c.Close) / 4

		var haOpen float64
		if i == 0 {
			haOpen = (c.Open + c.Close) / 2
		} else {
			haOpen = (prevHAOpen + prevHAClose) / 2
		}

		haHigh := math.Max(c.High, math.Max(haOpen, haClose))
		haLow := math.Min(c.Low, math.Min(haOpen, haClose))

		haCandles[i] = model.Candle{
			OpenTime: c.OpenTime,
			Open:     haOpen,
			High:     haHigh,
			Low:      haLow,
			Close:    haClose,
			Volume:   c.Volume,
		}

		prevHAOpen = haOpen
		prevHAClose = haClose
	}

	return haCandles
}
