package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis"
	"os"
	"time"
)

type Candlestick struct {
	OpenTime                 time.Time
	Open, High, Low, Close   float64
	Volume                   float64
	CloseTime                time.Time
	QuoteAssetVolume         float64
	TakerBuyBaseAssetVolume  float64
	TakerBuyQuoteAssetVolume float64
	SMA10, SMA30             float64
	RSI14                    float64
	Returns                  float64
}

type OrderBook struct {
	Bids []OrderBookEntry
	Asks []OrderBookEntry
}

type OrderBookEntry struct {
	Price    float64
	Quantity float64
}

type Trade struct {
	ID               int64
	Price            float64
	Quantity         float64
	BuyerIsMaker     bool
	Time             int64
	IsBestPriceMatch bool
}

type Cache struct {
	client *redis.Client
}

func NewCache() *Cache {
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"), // e.g. "localhost:6379"
		Password: os.Getenv("REDIS_PASS"), // no password set by default
		DB:       0,                       // use default DB
	})

	return &Cache{
		client: rdb,
	}
}

func (c *Cache) Set(key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	err = c.client.Set(context.Background(), key, data, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) Get(key string, target interface{}) (bool, error) {
	data, err := c.client.Get(context.Background(), key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	err = json.Unmarshal(data, target)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *Cache) Delete(key string) error {
	err := c.client.Del(context.Background(), key).Err()
	if err != nil {
		return err
	}
	return nil
}

func (c *Cache) UpdateKlines(symbol string, kline Candlestick) error {
	key := fmt.Sprintf("klines:%s", symbol)

	found, err := c.Get(key, &kline)
	if err != nil {
		return err
	}

	var klines []Candlestick
	if found {
		klines = append(klines, kline)
	} else {
		klines = []Candlestick{kline}
	}

	err = c.Set(key, klines, 24*time.Hour)
	if err != nil {
		return err
	}

	return nil
}
