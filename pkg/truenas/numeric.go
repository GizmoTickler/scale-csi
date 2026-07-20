package truenas

import "math"

const maxInt64FloatExclusive = float64(uint64(1) << 63)

func nonNegativeInt64FromFloat(value float64) (int64, bool) {
	if math.IsNaN(value) || value < 0 || value >= maxInt64FloatExclusive {
		return 0, false
	}
	return int64(value), true
}
