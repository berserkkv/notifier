package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"notifier/model"
)

const baseURL = "https://fapi.binance.com"

type BinanceClient struct {
	httpClient *http.Client
}

func NewBinanceClient() *BinanceClient {
	return &BinanceClient{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *BinanceClient) GetFuturesCandles(symbol, interval string, limit int) ([]model.Candle, error) {
	url := fmt.Sprintf(
		"%s/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
		baseURL,
		symbol,
		interval,
		limit,
	)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("binance error: %s", string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var raw [][]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	candles := make([]model.Candle, 0, len(raw))

	for _, k := range raw {
		openTime := int64(k[0].(float64))

		open, _ := strconv.ParseFloat(k[1].(string), 64)
		high, _ := strconv.ParseFloat(k[2].(string), 64)
		low, _ := strconv.ParseFloat(k[3].(string), 64)
		closePrice, _ := strconv.ParseFloat(k[4].(string), 64)
		volume, _ := strconv.ParseFloat(k[5].(string), 64)

		candles = append(candles, model.Candle{
			OpenTime: openTime,
			Open:     open,
			High:     high,
			Low:      low,
			Close:    closePrice,
			Volume:   volume,
		})
	}

	return candles, nil
}
