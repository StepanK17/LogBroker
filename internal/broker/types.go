package broker

type Topic struct {
	Name       string
	Partitions uint32
}

type Record struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}
