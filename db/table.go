package db

import (
	"fmt"
	"log/slog"
)

func (tbl *Table) AddColumn(colName string, vectorLength int64) (*Column, error) {
	currColCount := int64(len(tbl.columns))
	if currColCount >= tbl.meta.numColumns {
		slog.Error("Add column error", "Table", tbl.meta.name.String(), "Max columns", tbl.meta.numColumns)
		return nil, fmt.Errorf("Pain")
	}
	b := tbl.file.Bytes()

	// Check if we need to grow the file for a new chunk
	firstChunkOffset := GetDataCursorPos(b)
	if firstChunkOffset+ChunkSize > int64(len(b)) {
		if err := tbl.file.Grow(ChunkSize*4); err != nil {
			return nil, err
		}
		b = tbl.file.Bytes() // refresh after grow
	}

	pos := tbl.meta.offset + TableMetadataSize + (ColumnMetadataSize * currColCount)
	meta := ColumnMetadata{
		name:             MakeName(colName),
		vectorLength:     vectorLength,
		numVectors:       0,
		firstChunkOffset: firstChunkOffset,
		offset:           pos,
	}
	meta.WriteTo(b)

	newColumn := Column{
		meta: meta,
		file: tbl.file,
	}

	tbl.columns = append(tbl.columns, &newColumn)

	// move data cursor 64MB forward
	nextChunkPos := meta.firstChunkOffset + ChunkSize
	SetDataCursorPos(b, nextChunkPos, RIGHT)

	return &newColumn, nil
}

func (tbl *Table) ListColumnNames() []string {
	names := []string{}
	for _, col := range tbl.columns {
		names = append(names, col.meta.name.String())
	}
	return names
}
