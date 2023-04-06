package main

import (
	"encoding/json"
	"errors"
	"github.com/adshao/go-binance/v2"
	"github.com/redis/go-redis"
	"os"
	"time"
)

type Kline struct {
	Symbol string
	Kline  *binance.WsKline
}

type AggTrade struct {
	Symbol   string
	AggTrade *binance.WsAggTradeEvent
}

type Depth struct {
	Symbol string
	Depth  *binance.WsDepthEvent
}

type Cache struct {
	client *redis.Client
}

const (
	KlineKeyPrefix    = "kline:"
	TradeKeyPrefix    = "trade:"
	DepthKeyPrefix    = "depth:"
	KlineCacheMaxSize = 10000
	TradeCacheMaxSize = 100000
	DepthCacheMaxSize = 100000
)

// NewCache creates a new Cache instance with a Redis client.
func NewCache() (*Cache, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Password: os.Getenv("REDIS_PASS"),
		DB:       0,
	})

	_, err := rdb.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &Cache{
		client: rdb,
	}, nil
}

func (c *Cache) Set(key string, value interface{}, expiration time.Duration) error {
	if value == nil {
		return errors.New("value cannot be nil")
	}

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	pipe := c.client.Pipeline()
	pipe.Set(key, data, expiration)
	pipe.LPush(key+":list", data)
	pipe.LTrim(key+":list", 0, KlineCacheMaxSize-1)
	_, err = pipe.Exec()
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) Get(key string, target interface{}) (bool, error) {
	data, err := c.client.Get(key).Bytes()
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
	err := c.client.Del(key).Err()
	if err != nil {
		return err
	}
	return nil
}

func (c *Cache) UpdateKline(symbol string, klineData *Kline, expiration time.Duration) error {
	key := KlineKeyPrefix + symbol

	// Get the existing klines for the symbol from the cache
	var klines []*Kline
	_, err := c.Get(key, &klines)
	if err != nil {
		return err
	}

	// Append the new kline to the list
	klines = append(klines, klineData)

	// If the list is too long, remove the oldest kline
	if len(klines) > KlineCacheMaxSize {
		klines = klines[len(klines)-KlineCacheMaxSize:]
	}

	// Update the cache with the new klines
	err = c.Set(key, klines, expiration)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) UpdateTrade(symbol string, tradeData *AggTrade, expiration time.Duration) error {
	key := TradeKeyPrefix + symbol

	// Get the existing trades for the symbol from the cache
	var trades []*AggTrade
	_, err := c.Get(key, &trades)
	if err != nil {
		return err
	}

	// Append the new trade to the list
	trades = append(trades, tradeData)

	// If the list is too long, remove the oldest trade
	if len(trades) > TradeCacheMaxSize {
		trades = trades[len(trades)-TradeCacheMaxSize:]
	}

	// Update the cache with the new trades
	err = c.Set(key, trades, expiration)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) UpdateDepth(symbol string, depthData *Depth, expiration time.Duration) error {
	key := DepthKeyPrefix + symbol

	// Get the existing depth data for the symbol from the cache
	var depths []*Depth
	_, err := c.Get(key, &depths)
	if err != nil {
		return err
	}

	// Append the new depth data to the list
	depths = append(depths, depthData)

	// If the list is too long, remove the oldest depth data
	if len(depths) > DepthCacheMaxSize {
		depths = depths[len(depths)-DepthCacheMaxSize:]
	}

	// Update the cache with the new depth data
	err = c.Set(key, depths, expiration)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) GetKlines(symbol string, limit int) ([]*Kline, error) {
	key := KlineKeyPrefix + symbol

	// Get the klines for the symbol from the cache
	var klines []*Kline
	found, err := c.Get(key, &klines)
	if err != nil {
		return nil, err
	}

	// If the klines were not found in the cache, return an empty slice
	if !found {
		return []*Kline{}, nil
	}

	// Return the last `limit` klines
	if len(klines) > limit {
		klines = klines[len(klines)-limit:]
	}

	return klines, nil
}

func (c *Cache) GetTrades(symbol string, limit int) ([]*AggTrade, error) {
	key := TradeKeyPrefix + symbol

	// Get the trades for the symbol from the cache
	var trades []*AggTrade
	found, err := c.Get(key, &trades)
	if err != nil {
		return nil, err
	}

	// If the trades were not found in the cache, return an empty slice
	if !found {
		return []*AggTrade{}, nil
	}

	// Return the last `limit` trades
	if len(trades) > limit {
		trades = trades[len(trades)-limit:]
	}

	return trades, nil
}

func (c *Cache) GetDepth(symbol string, limit int) ([]*Depth, error) {
	key := DepthKeyPrefix + symbol

	// Get the depth data for the symbol from the cache
	var depths []*Depth
	found, err := c.Get(key, &depths)
	if err != nil {
		return nil, err
	}

	// If the depth data was not found in the cache, return an empty slice
	if !found {
		return []*Depth{}, nil
	}

	// Return the last `limit` depth data
	if len(depths) > limit {
		depths = depths[len(depths)-limit:]
	}

	return depths, nil
}
