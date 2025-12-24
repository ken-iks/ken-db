package parsers

type RecordURI struct {
	VideoID   string `parquet:"name=Video_id"`
	Timestamp int64  `parquet:"name=Timestamp"`
	GcsURI    string `parquet:"name=GcsURI"`
}

func (RecordURI) isRecord() {}

type RecordEmbedding struct {
	VideoID   string    `parquet:"Video_id"`
	Timestamp int64     `parquet:"Timestamp"`
	Embedding []float32 `parquet:"Embedding"`
}

func (RecordEmbedding) isRecord() {}

// Making the type self documenting so we can use switch by interface
type Record interface {
	isRecord()
}
