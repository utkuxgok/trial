package main

import (
	"context"
	"fmt"
	"github.com/adshao/go-binance/v2"
	"strconv"
	"sync"
	"time"
)

func parseFloat(value string, name string) (float64, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %s value: %w", name, err)
	}
	return parsed, nil
}

func klineToCandlestick(kline *binance.Kline) (*Candlestick, error) {
	openTime := time.Unix(kline.OpenTime/1000, 0)
	closeTime := time.Unix(kline.CloseTime/1000, 0)

	open, err := parseFloat(kline.Open, "open")
	if err != nil {
		return nil, err
	}

	high, err := parseFloat(kline.High, "high")
	if err != nil {
		return nil, err
	}

	low, err := parseFloat(kline.Low, "low")
	if err != nil {
		return nil, err
	}

	closePricing, err := parseFloat(kline.Close, "closePricing")
	if err != nil {
		return nil, err
	}

	volume, err := parseFloat(kline.Volume, "volume")
	if err != nil {
		return nil, err
	}

	quoteAssetVolume, err := parseFloat(kline.QuoteAssetVolume, "quoteAssetVolume")
	if err != nil {
		return nil, err
	}

	takerBuyBaseAssetVolume, err := parseFloat(kline.TakerBuyBaseAssetVolume, "takerBuyBaseAssetVolume")
	if err != nil {
		return nil, err
	}

	takerBuyQuoteAssetVolume, err := parseFloat(kline.TakerBuyQuoteAssetVolume, "takerBuyQuoteAssetVolume")
	if err != nil {
		return nil, err
	}

	return &Candlestick{
		OpenTime:                 openTime,
		Open:                     open,
		High:                     high,
		Low:                      low,
		Close:                    closePricing,
		Volume:                   volume,
		CloseTime:                closeTime,
		QuoteAssetVolume:         quoteAssetVolume,
		TakerBuyBaseAssetVolume:  takerBuyBaseAssetVolume,
		TakerBuyQuoteAssetVolume: takerBuyQuoteAssetVolume,
	}, nil
}

func processSymbols(client *binance.Client, symbols []string, interval string, limit int, doneFetching chan struct{}) *Cache {
	cache := NewCache() // 10, 10*time.Minute old arguments

	var wg sync.WaitGroup

	for _, symbol := range symbols {
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done()
			klines, err := fetchKlinesWithRetry(client, symbol, interval, limit, 3)
			if err != nil {
				fmt.Printf("Failed to retrieve data for %s after %d attempts\n", symbol, 3)
				return
			}
			errr := cache.Set(symbol, klines, time.Minute*5)
			if errr != nil {
				return
			}
		}(symbol)
	}
	wg.Wait()
	close(doneFetching) // Signal that the historical data has been fetched for all symbols
	return cache
}

func fetchKlinesWithRetry(client *binance.Client, symbol string, interval string, limit int, maxAttempts int) ([]Candlestick, error) {
	var klines []Candlestick

	err := withRetry(maxAttempts, func() error {
		var err error
		klines, err = fetchKlines(client, symbol, interval, limit)
		return err
	})

	return klines, err
}

func fetchKlines(client *binance.Client, symbol string, interval string, limit int) ([]Candlestick, error) {
	ctx := context.Background()
	binanceKlines, err := client.NewKlinesService().Symbol(symbol).Interval(interval).Limit(limit).Do(ctx)
	if err != nil {
		return nil, err
	}

	klines := make([]Candlestick, 0, len(binanceKlines))
	for _, kline := range binanceKlines {
		candlestick, err := klineToCandlestick(kline)
		if err != nil {
			return nil, err
		}
		klines = append(klines, *candlestick)
	}

	return klines, nil
}

func withRetry(maxRetries int, f func() error) error {
	for retries := 0; retries < maxRetries; retries++ {
		err := f()
		if err == nil {
			return nil
		}
		fmt.Printf("Error (attempt %d): %v\n", retries+1, err)
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("failed after %d attempts", maxRetries)
}
