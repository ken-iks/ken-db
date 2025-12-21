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
	pos := tbl.meta.offset + TableMetadataSize + (ColumnMetadataSize * currColCount)
	meta := ColumnMetadata{
		name: MakeName(colName),
		vectorLength: vectorLength,
		numVectors: 0,
		firstChunkOffset: GetDataCursorPos(tbl.mapped),
	}
	meta.WriteTo(tbl.mapped, pos)

	newColumn := Column{
		meta: meta,
		mapped: tbl.mapped,
	}

	tbl.columns = append(tbl.columns, &newColumn)

	// move data cursor 64MB forward
	nextChunkPos := meta.firstChunkOffset + ChunkSize
	SetDataCursorPos(tbl.mapped, nextChunkPos, RIGHT)

	return &newColumn, nil
}

func (tbl *Table) ListColumnNames() []string {
	names := []string{}
	for _, col := range tbl.columns {
		names = append(names, col.meta.name.String())
	}
	return names
}