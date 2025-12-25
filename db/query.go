package db

import (
	"log/slog"
	"math"
	"sync"

	"github.com/google/uuid"
)

// This method is a general parser for all queries that have somethng to do with
// some target vector
func ParseTargetQuery(q QueryBuilder, target []float32) *QueryOptions {
	// method gets its own local variable pool for operation
	pool := VariablePool{}
	switch q.kind {
	case IKEJI:
		varname := q.col.Ikeji(target, pool)
		return &QueryOptions{vectarr: q.col.Fetch(varname, pool)}
	default:
		slog.Error("Type not implemented", "query type", q.kind)
		return nil
	}
}

type timestampRange struct {
	start int64
	end int64
	score float32
}
// Implementation of the ikeji algorithm on a column
// Returns the variable name that the associated bitmap has been saved to
func (col *Column) Ikeji(target []float32, pool VariablePool) string {
	// global trackers
	var wg sync.WaitGroup
	bests := make([]timestampRange, col.Length())
	col.forEach(func(idx int64, ts uint64, vec []float32) bool {
		wg.Add(1)
		go func (idx int64, target []float32, col *Column) {
			defer wg.Done()
			bests[idx] = ikejiRange(col, idx, target)
		}(idx, target, col)
		return true
	})
	wg.Wait()
	minscore := float32(math.MaxFloat32)
	var minval timestampRange
	for _, r := range bests {
		if r.score < minscore {
			minval = r
		}
	}
	col.Select(minval.start, minval.end, "final", pool)
	return "final"
}

func ikejiRange(col *Column, startidx int64, target []float32) timestampRange {
	// define a local variable pool to use, and keep track of best
	pool := VariablePool{}
	currbestRange := timestampRange{}
	currbestScore := float32(math.MaxFloat32)

	col.forEach(func(idx int64, ts uint64, vec []float32) bool {
		if idx <= startidx {
			return true
		}
		// define check id, and add the selection of elements to the variable pool
		checkId := uuid.New().String()
		col.Select(startidx, idx, checkId, pool)
		// once the checkId bitmap is in the variable pool, we comput average distance from the target
		res := col.DistAvg(checkId, pool, target)
		if res < currbestScore {
			currbestRange = timestampRange{
				start: startidx,
				end: idx,
				score: res,
			}
			// anytime if window n+1 is closer than window n, keep going, if not just stop
			return true
		}
		return false
	})
	return currbestRange
}