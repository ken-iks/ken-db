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
	chunkPos := column.meta.firstChunkOffset
	header := ReadChunkHeader(column.mapped, column.meta.firstChunkOffset)
	for header.nextChunk != 0 {
		chunkPos = header.nextChunk
		header = ReadChunkHeader(column.mapped, header.nextChunk)
	}
	entrySize := int64(8) + (column.meta.vectorLength * 4)  // timestamp + vector
	vectorPos := chunkPos + ChunkHeaderSize + (entrySize * column.meta.numVectors)
	if (vectorPos + (4 * lenInt64)) - chunkPos <= ChunkSize {
		// we have enough space in this chunk to add the vector
        ByteOrder.PutUint64(column.mapped[vectorPos:], uint64(timestamp))
        writeVec(column.mapped[vectorPos+8:], vector)
        column.meta.numVectors++
		header.numVectors++
		// update mmap
		column.meta.WriteTo(column.mapped)
		header.WriteTo(column.mapped, chunkPos)
        return nil
	}
	// if we do not have enough space, then we must start a new chunk,
	// and add the vector to it
	newChunkPos := GetDataCursorPos(column.mapped)
	header.nextChunk = newChunkPos

	newChunkHeader := ChunkHeader{
		nextChunk: 0,
		numVectors: 0,
	}
	// set new chunk
	newChunkHeader.WriteTo(column.mapped, newChunkPos)
	
	// write vector
	ByteOrder.PutUint64(column.mapped[newChunkPos + ChunkHeaderSize:], uint64(timestamp))
	writeVec(column.mapped[newChunkPos + ChunkHeaderSize + 8:], vector)
	// update mmap after successful write
	newChunkHeader.numVectors++
	column.meta.numVectors++
	column.meta.WriteTo(column.mapped)
	newChunkHeader.WriteTo(column.mapped, newChunkPos)
	// also update the old header
	header.WriteTo(column.mapped, chunkPos)
	return nil
}

func (column *Column) PrintColumnEntries() {
    vectorSize := column.meta.vectorLength * 4
    entrySize := 8 + vectorSize

    currChunk := column.meta.firstChunkOffset
    for currChunk != 0 {
        header := ReadChunkHeader(column.mapped, currChunk)
        
        for i := int64(0); i < header.numVectors; i++ {
            entryOffset := currChunk + ChunkHeaderSize + (i * entrySize)
            ts := ByteOrder.Uint64(column.mapped[entryOffset:])
            vec := readVec(column.mapped[entryOffset+8:], int(column.meta.vectorLength))
            fmt.Println("Timestamp:", ts, "Vector:", vec)
        }
        
        currChunk = header.nextChunk  // move to next chunk (0 if none)
    }
}