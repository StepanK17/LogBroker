package storage

import (
	"encoding/binary"
	"io"
	"os"
)

type Index struct {
	file       *os.File
	path       string
	baseOffset uint64
}

func OpenIndex(path string, baseOffset uint64) (*Index, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	index := &Index{
		file:       file,
		path:       path,
		baseOffset: baseOffset,
	}
	return index, nil
}

func (i *Index) Append(relativeOffset uint32, positionInLog uint64) error {
	_, err := i.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], relativeOffset)
	binary.BigEndian.PutUint64(buf[4:12], positionInLog)

	_, err = i.file.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (i *Index) Find(relativeOffset uint32) (uint64, bool, error) {
	_, err := i.file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, false, err
	}

	buf := make([]byte, 12)
	for {
		_, err := io.ReadFull(i.file, buf)
		if err != nil {
			if err == io.EOF {
				return 0, false, nil
			}
			if err == io.ErrUnexpectedEOF {
				return 0, false, err
			}
			return 0, false, err
		}

		storedRelativeOffset := binary.BigEndian.Uint32(buf[0:4])
		storedPositionInLog := binary.BigEndian.Uint64(buf[4:12])

		if storedRelativeOffset == relativeOffset {
			return storedPositionInLog, true, nil
		}
	}
}

func (i *Index) Close() error {
	return i.file.Close()
}
