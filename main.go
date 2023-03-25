package main

import (
	"context"
	"fmt"
	"github.com/adshao/go-binance/v2"
	"os"
	"strings"
	"sync" // Added sync package for WaitGroup
)

func getAllUSDTTradingPairs(client *binance.Client) ([]string, error) {
	exchangeInfo, err := client.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		return nil, err
	}

	var usdtSymbols []string
	for _, symbol := range exchangeInfo.Symbols {
		if strings.HasSuffix(symbol.Symbol, "USDT") && !(strings.HasSuffix(symbol.Symbol, "UPUSDT")) && !(strings.HasSuffix(symbol.Symbol, "DOWNUSDT")) {
			usdtSymbols = append(usdtSymbols, symbol.Symbol)
		}
	}

	return usdtSymbols, nil
}

func main() {
	apiKey := os.Getenv("BINANCE_API_KEY")
	secretKey := os.Getenv("BINANCE_SECRET_KEY")

	client := binance.NewClient(apiKey, secretKey)

	// Fetch all USDT trading pairs
	symbols, err := getAllUSDTTradingPairs(client)
	if err != nil {
		fmt.Printf("Error fetching trading pairs: %v\n", err)
		return
	}

	interval := "1m"
	limit := 100

	// Initialize cache with historical data using REST API
	// Retrieve historical data using REST API
	doneFetching := make(chan struct{})
	cache := processSymbols(client, symbols, interval, limit, doneFetching)

	// Add a WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup

	// Start WebSocket routines for each symbol
	for _, symbol := range symbols {
		wg.Add(2) // Added 2 for each symbol for both websocketRoutine and orderBookWebSocketRoutine
		go websocketRoutine(cache, symbol, interval, &wg)
		go orderBookWebSocketRoutine(cache, symbol, &wg)
	}

	// Wait for all goroutines to complete
	wg.Wait()
}
