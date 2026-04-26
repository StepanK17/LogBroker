package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type Segment struct {
	baseOffset uint64
	file       *os.File
	path       string
	size       int64
	index      *Index
}

func OpenSegment(path string, baseOffset uint64) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	size := info.Size()

	indexPath := strings.TrimSuffix(path, ".log") + ".index"
	index, err := OpenIndex(indexPath, baseOffset)
	if err != nil {
		file.Close()
		return nil, err
	}

	res := &Segment{
		baseOffset: baseOffset,
		file:       file,
		path:       path,
		size:       size,
		index:      index,
	}
	return res, nil

}
func (s *Segment) Append(recordOffset uint64, encoded []byte) error {
	pos, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	_, err = s.file.Write(encoded)
	if err != nil {
		return err
	}
	s.size += int64(len(encoded))
	relativeOffset := uint32(recordOffset - s.baseOffset)
	err = s.index.Append(relativeOffset, uint64(pos))
	if err != nil {
		return err
	}
	return nil

}

func (s *Segment) ReadAll() ([]Record, error) {
	_, err := s.file.Seek(0, io.SeekStart)
	if err != nil {
		return []Record{}, err
	}

	return s.readRecordsFromCurrentPosition(-1)
}

func (s *Segment) ReadAllN(maxRecords int) ([]Record, error) {
	_, err := s.file.Seek(0, io.SeekStart)
	if err != nil {
		return []Record{}, err
	}

	return s.readRecordsFromCurrentPosition(maxRecords)
}

func (s *Segment) readRecordsFromCurrentPosition(maxRecords int) ([]Record, error) {
	if maxRecords == 0 {
		return []Record{}, nil
	}

	var records []Record
	for {
		if maxRecords > 0 && len(records) >= maxRecords {
			break
		}

		record, err := s.readNextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)

	}
	return records, nil
}

func (s *Segment) Close() error {
	err := s.file.Close()
	if err != nil {
		return err
	}
	err = s.index.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *Segment) readNextRecord() (Record, error) {
	header := make([]byte, headerSize)

	_, err := io.ReadFull(s.file, header)
	if err != nil {
		if err == io.EOF {
			return Record{}, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return Record{}, fmt.Errorf("truncated record header: %w", err)
		}
		return Record{}, err
	}

	keySize := binary.BigEndian.Uint32(header[16:20])
	valueSize := binary.BigEndian.Uint32(header[20:24])

	payload := make([]byte, int(keySize)+int(valueSize))
	_, err = io.ReadFull(s.file, payload)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Record{}, fmt.Errorf("truncated record payload: %w", err)
		}
		return Record{}, err
	}

	data := make([]byte, 0, headerSize+len(payload))
	data = append(data, header...)
	data = append(data, payload...)

	return DecodeRecord(data)
}

func (s *Segment) ReadFromOffset(offset uint64) ([]Record, error) {
	return s.ReadFromOffsetN(offset, -1)
}

func (s *Segment) ReadFromOffsetN(offset uint64, maxRecords int) ([]Record, error) {
	if maxRecords == 0 {
		return []Record{}, nil
	}

	if offset < s.baseOffset {
		return []Record{}, nil
	}

	relativeOffset := uint32(offset - s.baseOffset)
	positionInLog, found, err := s.index.Find(relativeOffset)
	if err != nil {
		return nil, err
	}
	if !found {
		return []Record{}, nil
	}

	_, err = s.file.Seek(int64(positionInLog), io.SeekStart)
	if err != nil {
		return nil, err
	}

	return s.readRecordsFromCurrentPosition(maxRecords)
}

func (s *Segment) Size() (int64, error) {
	logInfo, err := s.file.Stat()
	if err != nil {
		return 0, err
	}

	indexInfo, err := s.index.file.Stat()
	if err != nil {
		return 0, err
	}

	return logInfo.Size() + indexInfo.Size(), nil
}

func (s *Segment) Remove() error {
	err := s.Close()
	if err != nil {
		return err
	}

	err = os.Remove(s.path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	err = os.Remove(s.index.path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (s *Segment) RebuildIndexAndNextOffset() (uint64, error) {
	err := s.index.Close()
	if err != nil {
		return 0, err
	}
	err = os.Remove(s.index.path)
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	newIndex, err := OpenIndex(s.index.path, s.baseOffset)
	if err != nil {
		return 0, err
	}
	s.index = newIndex
	_, err = s.file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}

	nextOffset := s.baseOffset
	var validEnd int64

	for {
		pos, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}

		record, err := s.readNextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				if truncateErr := s.file.Truncate(validEnd); truncateErr != nil {
					return 0, truncateErr
				}
				if _, seekErr := s.file.Seek(validEnd, io.SeekStart); seekErr != nil {
					return 0, seekErr
				}
				s.size = validEnd
				break
			}
			return 0, err
		}

		relativeOffset := uint32(record.Offset - s.baseOffset)
		if err := s.index.Append(relativeOffset, uint64(pos)); err != nil {
			return 0, err
		}

		validEnd, err = s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		nextOffset = record.Offset + 1
	}

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	return nextOffset, nil
}
