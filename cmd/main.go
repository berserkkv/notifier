package main

import (
	"log"

	"notifier/client"
	"notifier/strategy"
)

func main() {
	binance := client.NewBinanceClient()

	candles, err := binance.GetFuturesCandles(
		"SOLUSDT",
		"15m",
		200,
	)
	if err != nil {
		log.Fatal(err)
	}

	st := strategy.NewEMAGapRSIStrategy("solusdt", "15m", 50, 200, 0.3, 0.2, 14)

	signal := st.Process(candles)

	log.Printf("Signal: %s", signal)
}
