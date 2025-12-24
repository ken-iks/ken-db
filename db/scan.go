package db

import (
	"log/slog"
)

// variable pool stores intermediate results during predicate evaluation
// stored as a bit vector, where res[i] represents whether or not the vector
// at the i'th index is included in the result
type VariablePool = map[string][]bool

type Vector struct {
	timestamp uint64
	features  []float32
}

func (column *Column) Select(startTs int64, endTs int64, varName string, pool VariablePool) {
	column.forEach(func(idx int64, ts uint64, vec []float32) {
		if ts >= uint64(startTs) && ts < uint64(endTs) {
			pool[varName] = append(pool[varName], true)
		} else {
			pool[varName] = append(pool[varName], false)
		}
	})
}

func (column *Column) Fetch(varName string, pool VariablePool) []Vector {
	bitmap, ok := pool[varName]
	retVec := []Vector{}
	if !ok {
		slog.Error("Could not find variable in variable pool", "variable name", varName)
		return []Vector{}
	}

	// note that we use a manual iteration pattern here instead of using the
	// ForEach helper for performance optimization. We only read the relevant
	// vector bytes into memory and ignore the rest
	b := column.file.Bytes()
	vectorSize := column.meta.vectorLength * 4
	entrySize := 8 + vectorSize

	currChunk, idx := column.meta.firstChunkOffset, 0
	for currChunk != 0 {
		header := ReadChunkHeader(b, currChunk)
		for i := int64(0); i < header.numVectors; i++ {
			if bitmap[idx] {
				entryOffset := currChunk + ChunkHeaderSize + (i * entrySize)
				retVec = append(retVec, Vector{
					timestamp: ByteOrder.Uint64(b[entryOffset:]),
					features:  readVec(b[entryOffset+8:], int(column.meta.vectorLength)),
				})
			}
			idx++
		}
		currChunk = header.nextChunk
	}
	return retVec
}

// Reduce a the vector by the function provided
// Performs the fucntion iteratively across vectors represented by the corresponding bitmap
func (column *Column) reduce(varName string, pool VariablePool, fn func(v1, v2 Vector) Vector) Vector {
	bitmap, ok := pool[varName]
	var retVec Vector
	first := true
	if !ok {
		slog.Error("Could not find variable in variable pool", "variable name", varName)
		return retVec
	}

	b := column.file.Bytes()
	vectorSize := column.meta.vectorLength * 4
	entrySize := 8 + vectorSize

	currChunk, idx := column.meta.firstChunkOffset, 0
	for currChunk != 0 {
		header := ReadChunkHeader(b, currChunk)
		for i := int64(0); i < header.numVectors; i++ {
			if bitmap[idx] {
				entryOffset := currChunk + ChunkHeaderSize + (i * entrySize)
				vec := Vector{
					timestamp: ByteOrder.Uint64(b[entryOffset:]),
					features:  readVec(b[entryOffset+8:], int(column.meta.vectorLength)),
				}
				if first {
					retVec = vec
					first = false
				} else {
					retVec = fn(vec, retVec)
				}
			}
			idx++
		}
		currChunk = header.nextChunk
	}
	return retVec
}
