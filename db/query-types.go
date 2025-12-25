package db

type QueryType int
const (
	IKEJI QueryType = iota
)

type QueryBuilder struct {
	col *Column
	// so far we only support the ikeji algorithm query
	kind QueryType
}

type QueryOptions struct {
	vectarr []Vector
	floatarr []float32
	single *Vector
	raw *float32
}

func (o QueryOptions) GetArrayFloat() ([]float32, bool) {
	if len(o.floatarr) > 0 {
		return o.floatarr, true
	}
	return nil, false
}

func (o QueryOptions) GetArrayVec() ([]Vector, bool) {
	if len(o.vectarr) > 0 {
		return o.vectarr, true
	}
	return nil, false
}

func (o QueryOptions) GetVector() (Vector, bool) {
	return *o.single, o.single != nil
}

func (o QueryOptions) GetRaw() (float32, bool) {
	return *o.raw, o.raw != nil
}