package db

import (
	"github.com/viterin/vek/vek32"
)

func (c *Column) Sum(varName string, pool VariablePool) Vector {
	return c.reduce(
		varName,
		pool,
		func(v1, v2 Vector) Vector {
			return Vector{
				timestamp: 0,
				features:  vek32.Add(v1.features, v2.features),
			}
		},
	)
}

func (c *Column) Prod(varName string, pool VariablePool) Vector {
	return c.reduce(
		varName,
		pool,
		func(v1, v2 Vector) Vector {
			return Vector{
				timestamp: 0,
				features:  vek32.Mul(v1.features, v2.features),
			}
		},
	)
}

func (c *Column) Avg(varName string, pool VariablePool) Vector {
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
	return Vector{
		timestamp: 0,
		features:  vek32.DivNumber(sum.features, float32(len(sum.features))),
	}
}

func (c *Column) AvgDist(varName string, pool VariablePool, target []float32) float32 {
	avg := c.Avg(varName, pool)
	return vek32.CosineSimilarity(avg.features, target)
}

func (c *Column) MinDist(varName string, pool VariablePool, target []float32) Vector {
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

func (c *Column) MaxDist(varName string, pool VariablePool, target []float32) Vector {
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
