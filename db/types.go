package db

import "github.com/edsrzf/mmap-go"

// TODO: rewrite column types for image embed workflows
const (
	Int64Size = 8 // for timestamps
	Float64Size = 8 // for values
	NameSize  = 16 // two int64s
)

type MMap = mmap.MMap

type Name struct {
	offset int64
	length int64
}

type Column struct {
	name   Name
	offset int64
	size   int64
	mapped MMap
}

type Table struct {
	name    Name
	columns []*Column
	mapped  MMap
}

type DB struct {
	tables []*Table
	mapped MMap
}
