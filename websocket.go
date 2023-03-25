package main

import (
	"fmt"
	"github.com/adshao/go-binance/v2"
	"log"
	"math"
	"strconv"
	"sync"
	"time"
)

func logWsError(prefix string, symbol string, err error) {
	fmt.Printf("%s error for symbol %s: %v\n", prefix, symbol, err)
}

func websocketRoutine(cache *Cache, symbol string, interval string, wg *sync.WaitGroup) {
	defer wg.Done()

	tradeChan := make(chan float64, 1)

	wg.Add(2) // Increment wait group counter for the two new goroutines
	go startTradeWebSocket(symbol, tradeChan, wg)
	go startKlineWebSocket(symbol, interval, cache, tradeChan, wg)
}

func startTradeWebSocket(symbol string, tradeChan chan float64, wg *sync.WaitGroup) {
	defer wg.Done()
	attempt := 0
	for {
		doneC, _, err := binance.WsAggTradeServe(symbol, func(event *binance.WsAggTradeEvent) {
			price, err := strconv.ParseFloat(event.Price, 64)
			if err != nil {
				log.Printf("Error parsing trade price for symbol %s: %v\n", symbol, err)
				return
			}
			tradeChan <- price
		}, func(err error) {
			logWsError("WebSocket (trade channel)", symbol, err)
		})

		if err != nil {
			logWsError("WebSocket (trade channel)", symbol, err)
			return
		}

		<-doneC
		attempt++
		sleepWithBackoff(attempt)
	}
}

func startKlineWebSocket(symbol string, interval string, cache *Cache, tradeChan chan float64, wg *sync.WaitGroup) {
	defer wg.Done()
	attempt := 0
	for {
		doneC, _, err := binance.WsKlineServe(symbol, interval, func(event *binance.WsKlineEvent) {
			closePrice := event.Kline.Close
			if !event.Kline.IsFinal {
				select {
				case tradePrice, ok := <-tradeChan:
					if !ok {
						log.Printf("Error: tradeChan closed unexpectedly for symbol %s\n", symbol)
						return
					}
					closePrice = strconv.FormatFloat(tradePrice, 'f', -1, 64)
				default:
				}
			}

			kline := &binance.Kline{
				OpenTime:                 event.Kline.StartTime,
				Open:                     event.Kline.Open,
				High:                     event.Kline.High,
				Low:                      event.Kline.Low,
				Close:                    closePrice,
				Volume:                   event.Kline.Volume,
				CloseTime:                event.Kline.EndTime,
				QuoteAssetVolume:         event.Kline.QuoteVolume,
				TakerBuyBaseAssetVolume:  event.Kline.ActiveBuyVolume,
				TakerBuyQuoteAssetVolume: event.Kline.ActiveBuyQuoteVolume,
			}

			candlestick, err := klineToCandlestick(kline)
			if err != nil {
				log.Printf("Error converting kline for symbol %s: %v\n", symbol, err)
				return
			}

			errr := cache.UpdateKlines(symbol, *candlestick)
			if errr != nil {
				return
			}
			fmt.Printf("Received update for %s: %+v\n", symbol, candlestick)
		}, nil)

		if err != nil {
			logWsError("WebSocket (kline channel)", symbol, err)
			return
		}

		<-doneC
		attempt++
		sleepWithBackoff(attempt)
	}
}

func orderBookWebSocketRoutine(orderBookCache *Cache, symbol string, wg *sync.WaitGroup) {
	// Make sure to call wg.Done() when the function exits
	defer wg.Done()

	depthChan := make(chan *binance.WsDepthEvent, 1)

	go startOrderBookWebSocket(symbol, depthChan)

	for {
		select {
		case depthEvent := <-depthChan:
			// Process the depthEvent data
			fmt.Printf("Received order book update for %s\n", symbol)
			orderBook, err := depthEventToOrderBook(depthEvent)
			if err != nil {
				log.Printf("Error converting depth event for symbol %s: %v\n", symbol, err)
				continue
			}
			err = orderBookCache.Set(symbol, orderBook, time.Minute*5) // IMPORTANT ! This place is suspicious, it is necessary to investigate and know it thoroughly.
			if err != nil {
				log.Printf("Error updating order book cache for symbol %s: %v\n", symbol, err)
			}
		}
	}
}

func startOrderBookWebSocket(symbol string, depthChan chan *binance.WsDepthEvent) {
	attempt := 0
	for {
		doneC, _, err := binance.WsDepthServe(symbol, func(event *binance.WsDepthEvent) {
			depthChan <- event
		}, func(err error) {
			logWsError("WebSocket (order book channel)", symbol, err)
		})

		if err != nil {
			log.Printf("Error initializing WebSocket for symbol %s (order book channel): %v\n", symbol, err)
			return
		}

		<-doneC
		attempt++
		sleepWithBackoff(attempt)
	}
}

func depthEventToOrderBook(event *binance.WsDepthEvent) (*OrderBook, error) {
	bids, err := depthItemsToOrderBookEntries(event.Bids, nil)
	if err != nil {
		return nil, err
	}
	asks, err := depthItemsToOrderBookEntries(nil, event.Asks)
	if err != nil {
		return nil, err
	}

	return &OrderBook{
		Bids: bids,
		Asks: asks,
	}, nil
}

func depthItemsToOrderBookEntries(bids []binance.Bid, asks []binance.Ask) ([]OrderBookEntry, error) {
	items := make([]OrderBookEntry, 0, len(bids)+len(asks))
	for _, item := range bids {
		price, err := strconv.ParseFloat(item.Price, 64)
		if err != nil {
			return nil, err
		}
		quantity, err := strconv.ParseFloat(item.Quantity, 64)
		if err != nil {
			return nil, err
		}
		items = append(items, OrderBookEntry{Price: price, Quantity: quantity})
	}

	for _, item := range asks {
		price, err := strconv.ParseFloat(item.Price, 64)
		if err != nil {
			return nil, err
		}
		quantity, err := strconv.ParseFloat(item.Quantity, 64)
		if err != nil {
			return nil, err
		}
		items = append(items, OrderBookEntry{Price: price, Quantity: quantity})
	}

	return items, nil
}

func sleepWithBackoff(attempt int) {
	backoff := math.Pow(2, float64(attempt))
	sleepDuration := time.Duration(backoff) * time.Second
	time.Sleep(sleepDuration)
}
