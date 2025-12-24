package db

import (
	"fmt"
	"log/slog"
	"unsafe"
)

type Kind int

const (
	Bytes Kind = iota
	Floats
)

type WriteColumnOptions struct {
	Kind   Kind
	bytes  []byte
	floats []float32
}

func (opts *WriteColumnOptions) AddBytes(b []byte) error {
	if opts.Kind == Bytes {
		opts.bytes = b
		return nil
	}
	return fmt.Errorf("Can only add bytes to a byte kind")
}

func (opts *WriteColumnOptions) AddFloats(f []float32) error {
	if opts.Kind == Floats {
		opts.floats = f
		return nil
	}
	return fmt.Errorf("Can only add floats to a float kind")
}

// returns the int64 length of the array for looping
func safeParse(v WriteColumnOptions, vectorLength int64) (int64, error) {
	switch v.Kind {
	case Floats:
		l := int64(len(v.floats))
		if l != vectorLength {
			slog.Error("Cannot add vector to column", "Vector length: ", l, "Column vector length: ", vectorLength)
			return 0, fmt.Errorf("Bruh")
		}
		// since floats are 4 bytes we multiply
		return l * 4, nil
	case Bytes:
		l := int64(len(v.bytes))
		if l != vectorLength*4 {
			slog.Error("Illegal byte array trying to be added to column", "array length:", l)
			return 0, fmt.Errorf("Bruh")
		}
		return l, nil
	default:
		slog.Error("Unsupported vector type", "type", v.Kind)
		return 0, fmt.Errorf("Bruh")
	}
}

// This helper assumes that this vector has been validated and can be added
// to the byte array at the beginning. Caller must give correct slice.
func writeVec[T float32 | byte](b []byte, vec []T) {
	size := int(unsafe.Sizeof(vec[0]))
	src := unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*size)
	copy(b, src)
}

// This helper reads directly from mmap with zero copy
// Modifying this slice would directly modify the mmap, so dont!
func readVec(b []byte, length int) []float32 {
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), length)
}

func (column *Column) AddVector(timestamp int64, vector WriteColumnOptions) error {
	lenInt64, err := safeParse(vector, column.meta.vectorLength)
	if err != nil {
		return err
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
	if (vectorPos+lenInt64+8)-chunkPos <= ChunkSize {
		// we have enough space in this chunk to add the vector
		ByteOrder.PutUint64(b[vectorPos:], uint64(timestamp))
		switch vector.Kind {
		case Bytes:
			writeVec(b[vectorPos+8:], vector.bytes)
		case Floats:
			writeVec(b[vectorPos+8:], vector.floats)
		default:
			return fmt.Errorf("Bruh")
		}
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
	switch vector.Kind {
	case Bytes:
		writeVec(b[newChunkPos+ChunkHeaderSize+8:], vector.bytes)
	case Floats:
		writeVec(b[newChunkPos+ChunkHeaderSize+8:], vector.floats)
	default:
		return fmt.Errorf("Bruh")
	}

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

// TODO: Implement some mathemtical functions - SUM, AVG, MIN, MAX
// These will be both vector operations (so V1+V2) and intra vector (eg AVG of V1)
// Also need a dot prod (vector distance)
// logistic regression
// method for adding a timestsamp row and for adding a table row and for adding a vector row (add vector for each column in table)
