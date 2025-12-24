package db

import (
	"github.com/viterin/vek/vek32"
)

type DistanceCondition int
const (
	AVG DistanceCondition = iota
	MAX
	MIN
)

type ReducerCondition int
const (
	SUM ReducerCondition = iota
	PROD
)

// TODO: finish building query parser by having a condition builder. This build will be separate from
// the actual selecter - just does math. Select and fetch can be defined in query.go since they are to 
// get the actual results. Maybe we eventually move here to give less work to the caller
type Condition struct {

}

type QueryOptions struct {
	floatarr []float32
	single *Vector
	raw *float32
}

func (o QueryOptions) GetArray() ([]float32, bool) {
	if len(o.floatarr) > 0 {
		return o.floatarr, true
	}
	return nil, false
}

func (o QueryOptions) GetVector() (Vector, bool) {
	return *o.single, o.single != nil
}

func (o QueryOptions) GetRaw() (float32, bool) {
	return *o.raw, o.raw != nil
}

// Returns the element-wise sum of all vectors in the set
func (c *Column) Sum(varName string, pool VariablePool) []float32 {
	return c.reduce(
		varName,
		pool,
		func(v1, v2 Vector) Vector {
			return Vector{
				timestamp: 0,
				features:  vek32.Add(v1.features, v2.features),
			}
		},
	).features
}

// Returns the element-wise product of all vectors in the set
func (c *Column) Prod(varName string, pool VariablePool) []float32 {
	return c.reduce(
		varName,
		pool,
		func(v1, v2 Vector) Vector {
			return Vector{
				timestamp: 0,
				features:  vek32.Mul(v1.features, v2.features),
			}
		},
	).features
}

// Returns the average distance from the set of vectors to the target
func (c *Column) DistAvg(varName string, pool VariablePool, target []float32) float32 {
	// first compute general average - then compute similarity
	// centroid distance is preffered to average of distances in the case of vector similarity
	avg := c.avg(varName, pool)
	return vek32.CosineSimilarity(avg, target)
}

// Returns the vector from the set with the minimum euclidean distance from the target
// Return value has {features []float32, timestamp int64}
func (c *Column) DistMin(varName string, pool VariablePool, target []float32) Vector {
	return c.reduce(
		varName,
		pool,
		func(v1, v2 Vector) Vector {
			if vek32.CosineSimilarity(v1.features, target) > vek32.CosineSimilarity(v2.features, target) {
				return v2
			}
			return v1
		},
	)
}

// Returns the vector from the set with the maximum euclidean distance from the target
// Return value has {features []float32, timestamp int64}
func (c *Column) DistMax(varName string, pool VariablePool, target []float32) Vector {
	return c.reduce(
		varName,
		pool,
		func(v1, v2 Vector) Vector {
			if vek32.CosineSimilarity(v1.features, target) > vek32.CosineSimilarity(v2.features, target) {
				return v1
			}
			return v2
		},
	)
}

// Helper for computing the element-wise average of a set of variables
// TODO: implement a running average (nice to have)
func (c *Column) avg(varName string, pool VariablePool) []float32 {
	sum := c.reduce(
		varName,
		pool,
		func(v1, v2 Vector) Vector {
			return Vector{
				timestamp: 0,
				features:  vek32.Add(v1.features, v2.features),
			}
		},
	)
	return vek32.DivNumber(sum.features, float32(len(sum.features)))
}
