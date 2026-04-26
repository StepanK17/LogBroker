package storage

type Record struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}
