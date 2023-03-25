package main

import "math"

func calculateSMA(data []float64, window int) []float64 {
	if len(data) < window {
		return nil
	}
	sma := make([]float64, len(data))
	sum := 0.0
	for i := 0; i < window; i++ {
		sum += data[i]
	}
	sma[window-1] = sum / float64(window)

	for i := window; i < len(data); i++ {
		sum += data[i] - data[i-window]
		sma[i] = sum / float64(window)
	}
	return sma
}

func calculateRSI(data []float64, window int) []float64 {
	if len(data) < window+1 {
		return nil
	}
	rsi := make([]float64, len(data))

	gain, loss := 0.0, 0.0
	for i := 1; i <= window; i++ {
		change := data[i] - data[i-1]
		if change > 0 {
			gain += change
		} else {
			loss -= change
		}
	}

	avgGain := gain / float64(window)
	avgLoss := loss / float64(window)

	rs := avgGain / avgLoss
	rsi[window] = 100 - (100 / (1 + rs))

	for i := window + 1; i < len(data); i++ {
		change := data[i] - data[i-1]
		if change > 0 {
			avgGain = (avgGain*(float64(window)-1) + change) / float64(window)
		} else {
			avgLoss = (avgLoss*(float64(window)-1) - change) / float64(window)
		}
		rs = avgGain / avgLoss
		rsi[i] = 100 - (100 / (1 + rs))
	}

	return rsi
}

func calculateReturns(data []float64, window int) []float64 {
	if len(data) < window {
		return nil
	}
	returns := make([]float64, len(data))
	for i := 1; i < len(data); i++ {
		returns[i] = (data[i] - data[i-1]) / data[i-1] * 100
	}
	return returns
}

func calculateEMA(data []float64, window int) []float64 {
	if len(data) < window {
		return nil
	}
	ema := make([]float64, len(data))
	multiplier := 2 / (float64(window) + 1)
	sma := calculateSMA(data, window)
	ema[window-1] = sma[window-1]

	for i := window; i < len(data); i++ {
		ema[i] = (data[i]-ema[i-1])*multiplier + ema[i-1]
	}
	return ema
}

func calculateMACD(data []float64, shortWindow, longWindow, signalWindow int) ([]float64, []float64, []float64) {
	if len(data) < longWindow+signalWindow {
		return nil, nil, nil
	}
	shortEMA := calculateEMA(data, shortWindow)
	longEMA := calculateEMA(data, longWindow)
	macdLine := make([]float64, len(data))
	for i := longWindow; i < len(data); i++ {
		macdLine[i] = shortEMA[i] - longEMA[i]
	}
	signalLine := calculateEMA(macdLine, signalWindow)
	histogram := make([]float64, len(data))
	for i := signalWindow; i < len(data); i++ {
		histogram[i] = macdLine[i] - signalLine[i]
	}
	return macdLine, signalLine, histogram
}

func calculateBollingerBands(data []float64, window int, numStdDev float64) ([]float64, []float64, []float64) {
	if len(data) < window {
		return nil, nil, nil
	}

	sma := calculateSMA(data, window)
	upperBand := make([]float64, len(data))
	lowerBand := make([]float64, len(data))

	for i := window - 1; i < len(data); i++ {
		sumOfSquaredDeviations := 0.0
		for j := 0; j < window; j++ {
			deviation := data[i-j] - sma[i]
			sumOfSquaredDeviations += deviation * deviation
		}
		variance := sumOfSquaredDeviations / float64(window)
		stdDev := math.Sqrt(variance)

		upperBand[i] = sma[i] + numStdDev*stdDev
		lowerBand[i] = sma[i] - numStdDev*stdDev
	}

	return upperBand, sma, lowerBand
}

func calculateVolumeWeightedMovingAverage(data, volume []float64, window int) []float64 {
	if len(data) < window || len(volume) < window {
		return nil
	}
	vwma := make([]float64, len(data))
	vwSum := 0.0
	vSum := 0.0

	for i := 0; i < window; i++ {
		vwSum += data[i] * volume[i]
		vSum += volume[i]
	}
	vwma[window-1] = vwSum / vSum

	for i := window; i < len(data); i++ {
		vwSum += (data[i] * volume[i]) - (data[i-window] * volume[i-window])
		vSum += volume[i] - volume[i-window]
		vwma[i] = vwSum / vSum
	}
	return vwma
}

func calculateMomentum(data []float64, window int) []float64 {
	if len(data) < window {
		return nil
	}
	momentum := make([]float64, len(data))
	for i := window; i < len(data); i++ {
		momentum[i] = data[i] - data[i-window]
	}
	return momentum
}

func calculateWMA(data []float64, window int) []float64 {
	if len(data) < window {
		return nil
	}
	wma := make([]float64, len(data)-window+1)
	denominator := float64(window * (window + 1) / 2)

	for i := 0; i < len(data)-window+1; i++ {
		numerator := 0.0
		for j := 0; j < window; j++ {
			numerator += data[i+j] * float64(j+1)
		}
		wma[i] = numerator / denominator
	}
	return wma
}

func calculateHMA(data []float64, window int) []float64 {
	if len(data) < window {
		return nil
	}
	hma := make([]float64, len(data))

	wmaHalf := calculateWMA(data, window/2)
	wmaFull := calculateWMA(data, window)

	points := make([]float64, len(data))
	for i := 0; i < len(data); i++ {
		points[i] = 2*wmaHalf[i] - wmaFull[i]
	}

	hma = calculateWMA(points, int(math.Sqrt(float64(window))))
	return hma
}

func calculateATR(high, low, close []float64, window int) []float64 {
	if len(high) < window || len(low) < window || len(close) < window {
		return nil
	}
	atr := make([]float64, len(high))
	tr := make([]float64, len(high))

	for i := 1; i < len(high); i++ {
		tr1 := high[i] - low[i]
		tr2 := math.Abs(high[i] - close[i-1])
		tr3 := math.Abs(low[i] - close[i-1])
		tr[i] = math.Max(tr1, math.Max(tr2, tr3))
	}

	atr[window-1] = calculateSMA(tr, window)[window-1]
	k := 1.0 / float64(window)

	for i := window; i < len(high); i++ {
		atr[i] = (1-k)*atr[i-1] + k*tr[i]
	}

	return atr
}

func calculateChaikinVolatility(highs, lows []float64, window int) []float64 {
	if len(highs) < window || len(lows) < window {
		return nil
	}

	chaikinVolatility := make([]float64, len(highs))
	for i := window; i < len(highs); i++ {
		sum := 0.0
		for j := i - window; j < i; j++ {
			sum += highs[j] - lows[j]
		}
		chaikinVolatility[i] = sum / float64(window)
	}
	return chaikinVolatility
}

func calculateADX(data []Candlestick, window int) []float64 {
	if len(data) < 2*window {
		return nil
	}

	adx := make([]float64, len(data))
	dmPlus := make([]float64, len(data))
	dmMinus := make([]float64, len(data))
	tr := make([]float64, len(data))

	// Calculate True Range, DM+, DM-
	for i := 1; i < len(data); i++ {
		tr[i] = math.Max(math.Max(data[i].High-data[i].Low, math.Abs(data[i].High-data[i-1].Close)), math.Abs(data[i].Low-data[i-1].Close))
		dmPlus[i] = math.Max(data[i].High-data[i-1].High, 0)
		dmMinus[i] = math.Max(data[i-1].Low-data[i].Low, 0)

		if dmPlus[i] < dmMinus[i] {
			dmPlus[i] = 0
		} else if dmPlus[i] == dmMinus[i] {
			dmPlus[i] = 0
			dmMinus[i] = 0
		} else {
			dmMinus[i] = 0
		}
	}

	// Calculate the smoothed values for TR, DM+ and DM-
	smoothedTR := calculateEMA(tr[1:], window)
	smoothedDMPlus := calculateEMA(dmPlus[1:], window)
	smoothedDMMinus := calculateEMA(dmMinus[1:], window)

	// Calculate DX
	dx := make([]float64, len(data))
	for i := window; i < len(data); i++ {
		diPlus := smoothedDMPlus[i-1] / smoothedTR[i-1]
		diMinus := smoothedDMMinus[i-1] / smoothedTR[i-1]

		dx[i] = math.Abs(diPlus-diMinus) / (diPlus + diMinus) * 100
	}

	// Calculate ADX
	adx = calculateEMA(dx[window:], window)

	return adx
}

func calculateStochasticOscillator(data []Candlestick, window int) ([]float64, []float64) {
	if len(data) < window {
		return nil, nil
	}

	k := make([]float64, len(data))
	d := make([]float64, len(data))

	for i := window - 1; i < len(data); i++ {
		lowest := data[i-window+1].Low
		highest := data[i-window+1].High

		for j := i - window + 2; j <= i; j++ {
			if data[j].Low < lowest {
				lowest = data[j].Low
			}
			if data[j].High > highest {
				highest = data[j].High
			}
		}

		k[i] = ((data[i].Close - lowest) / (highest - lowest)) * 100
	}

	// Calculate the simple moving average of %K values
	d = calculateSMA(k, window)

	return k, d
}

func calculateParabolicSAR(high, low []float64) []float64 {
	length := len(high)
	if length != len(low) {
		return nil
	}

	psar := make([]float64, length)
	isLong := true
	af := 0.02
	ep := 0.0
	hp := 0.0
	lp := 0.0

	psar[0] = low[0]

	for i := 1; i < length; i++ {
		psar[i] = psar[i-1] + af*(ep-psar[i-1])

		if isLong {
			if high[i] > hp {
				hp = high[i]
				af = math.Min(af+0.02, 0.2)
			}
			if low[i] <= psar[i] {
				isLong = false
				af = 0.02
				ep = low[i]
				lp = low[i]
				psar[i] = hp
			} else {
				ep = hp
			}
		} else {
			if low[i] < lp {
				lp = low[i]
				af = math.Min(af+0.02, 0.2)
			}
			if high[i] >= psar[i] {
				isLong = true
				af = 0.02
				ep = high[i]
				hp = high[i]
				psar[i] = lp
			} else {
				ep = lp
			}
		}
	}
	return psar
}
