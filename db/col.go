package db

import (
	"fmt"
	"log/slog"
	"unsafe"
)

// This helper assumes that this vector has been validated and can be added
// to the byte array at the beginning. Caller must give correct slice.
func writeVec(b []byte, vec []float32) {
	src := unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*4)
	copy(b, src)
}

// This helper reads directly from mmap with zero copy
// Modifying this slice would directly modify the mmap, so dont!
func readVec(b []byte, length int) []float32 {
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), length)
}

func (column *Column) AddVector(timestamp int64, vector []float32) error {
	lenInt64 := int64(len(vector))
	if column.meta.vectorLength != lenInt64 {
		slog.Error("Cannot add vector to column", "Vector length: ", len(vector), "Column vector length: ", column.meta.vectorLength)
		return fmt.Errorf("Bruh")
	}
	b := column.file.Bytes()
	chunkPos := column.meta.firstChunkOffset
	header := ReadChunkHeader(b, column.meta.firstChunkOffset)
	for header.nextChunk != 0 {
		chunkPos = header.nextChunk
		header = ReadChunkHeader(b, header.nextChunk)
	}
	entrySize := int64(8) + (column.meta.vectorLength * 4) // timestamp + vector
	vectorPos := chunkPos + ChunkHeaderSize + (entrySize * header.numVectors)
	if (vectorPos+(4*lenInt64))-chunkPos <= ChunkSize {
		// we have enough space in this chunk to add the vector
		ByteOrder.PutUint64(b[vectorPos:], uint64(timestamp))
		writeVec(b[vectorPos+8:], vector)
		column.meta.numVectors++
		header.numVectors++
		// update mmap
		column.meta.WriteTo(b)
		header.WriteTo(b, chunkPos)
		return nil
	}
	// if we do not have enough space, then we must start a new chunk,
	// and add the vector to it
	newChunkPos := GetDataCursorPos(b)
	// Check if file needs to grow
	if newChunkPos+ChunkSize > int64(len(b)) {
		if err := column.file.Grow(ChunkSize * 4); err != nil {
			return err
		}
		b = column.file.Bytes() // refresh after grow
	}
	header.nextChunk = newChunkPos

	newChunkHeader := ChunkHeader{
		nextChunk:  0,
		numVectors: 0,
	}
	// set new chunk
	newChunkHeader.WriteTo(b, newChunkPos)

	// write vector
	ByteOrder.PutUint64(b[newChunkPos+ChunkHeaderSize:], uint64(timestamp))
	writeVec(b[newChunkPos+ChunkHeaderSize+8:], vector)
	// update mmap after successful write
	newChunkHeader.numVectors++
	column.meta.numVectors++
	column.meta.WriteTo(b)
	newChunkHeader.WriteTo(b, newChunkPos)
	// also update the old header
	header.WriteTo(b, chunkPos)
	return nil
}

func (column *Column) forEach(fn func(idx int64, ts uint64, vec []float32)) {
	b := column.file.Bytes()
	entrySize := 8 + (column.meta.vectorLength * 4)
	idx := int64(0)

	currChunk := column.meta.firstChunkOffset
	for currChunk != 0 {
		header := ReadChunkHeader(b, currChunk)
		for i := int64(0); i < header.numVectors; i++ {
			entryOffset := currChunk + ChunkHeaderSize + (i * entrySize)
			ts := ByteOrder.Uint64(b[entryOffset:])
			vec := readVec(b[entryOffset+8:], int(column.meta.vectorLength))
			fn(idx, ts, vec)
			idx++
		}
		currChunk = header.nextChunk
	}
}

func (column *Column) PrintColumnEntries() {
	column.forEach(func(idx int64, ts uint64, vec []float32) {
		fmt.Println("Timestamp:", ts, "Vector:", vec)
	})
}
