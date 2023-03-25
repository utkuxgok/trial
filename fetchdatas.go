package main

import (
	"context"
	"github.com/adshao/go-binance/v2"
	"strconv"
)

func fetchOrderBook(client *binance.Client, symbol string) (*OrderBook, error) {
	depth, err := client.NewDepthService().Symbol(symbol).Limit(100).Do(context.Background())
	if err != nil {
		return nil, err
	}

	bids, err := depthItemsToOrderBookEntries(depth.Bids, nil)
	if err != nil {
		return nil, err
	}
	asks, err := depthItemsToOrderBookEntries(nil, depth.Asks)
	if err != nil {
		return nil, err
	}

	return &OrderBook{
		Bids: bids,
		Asks: asks,
	}, nil
}

func fetchTradeHistory(client *binance.Client, symbol string) ([]Trade, error) {
	trades, err := client.NewAggTradesService().Symbol(symbol).Limit(1000).Do(context.Background())
	if err != nil {
		return nil, err
	}

	tradeHistory := make([]Trade, 0, len(trades))
	for _, aggTrade := range trades {
		price, err := strconv.ParseFloat(aggTrade.Price, 64)
		if err != nil {
			return nil, err
		}
		quantity, err := strconv.ParseFloat(aggTrade.Quantity, 64)
		if err != nil {
			return nil, err
		}

		trade := Trade{
			ID:               aggTrade.AggTradeID,
			Price:            price,
			Quantity:         quantity,
			BuyerIsMaker:     aggTrade.IsBuyerMaker,
			Time:             aggTrade.Timestamp,
			IsBestPriceMatch: aggTrade.IsBestPriceMatch,
		}
		tradeHistory = append(tradeHistory, trade)
	}

	return tradeHistory, nil
}

func fetchKlines_(client *binance.Client, symbol, interval string, limit int) ([]Candlestick, error) {
	klines, err := client.NewKlinesService().Symbol(symbol).Interval(interval).Limit(limit).Do(context.Background())
	if err != nil {
		return nil, err
	}

	candlesticks := make([]Candlestick, 0, len(klines))
	for _, kline := range klines {
		candlestick, err := klineToCandlestick(kline)
		if err != nil {
			return nil, err
		}
		candlesticks = append(candlesticks, *candlestick)
	}

	return candlesticks, nil
}
