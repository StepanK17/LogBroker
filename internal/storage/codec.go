package storage

import (
	"encoding/binary"
	"fmt"
	"math"
)

const headerSize = 24

func EncodeRecord(r Record) ([]byte, error) {
	if len(r.Key) > math.MaxUint32 || len(r.Value) > math.MaxUint32 {
		return nil, fmt.Errorf("key or value is too large")
	}

	buf := make([]byte, headerSize+len(r.Key)+len(r.Value))

	binary.BigEndian.PutUint64(buf[0:8], r.Offset)
	binary.BigEndian.PutUint64(buf[8:16], uint64(r.Timestamp))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(r.Key)))
	binary.BigEndian.PutUint32(buf[20:24], uint32(len(r.Value)))

	copy(buf[24:24+len(r.Key)], r.Key)
	copy(buf[24+len(r.Key):], r.Value)
	return buf, nil

}

func DecodeRecord(data []byte) (Record, error) {
	if len(data) < headerSize {
		return Record{}, fmt.Errorf("record is smaller than header")
	}

	offset := binary.BigEndian.Uint64(data[0:8])
	timestamp := int64(binary.BigEndian.Uint64(data[8:16]))
	keySize := binary.BigEndian.Uint32(data[16:20])
	valueSize := binary.BigEndian.Uint32(data[20:24])
	keyStart := headerSize
	keyEnd := keyStart + int(keySize)
	valueStart := keyEnd
	valueEnd := valueStart + int(valueSize)
	if len(data) < valueEnd {
		return Record{}, fmt.Errorf("record payload is truncated")
	}

	key := append([]byte(nil), data[keyStart:keyEnd]...)
	value := append([]byte(nil), data[valueStart:valueEnd]...)
	result := Record{
		Offset:    offset,
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
	}
	return result, nil

}
