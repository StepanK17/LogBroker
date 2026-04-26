package storage

import (
	"fmt"
	"sync"

	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultRetentionBytes = 10 << 20
const defaultSegmentMaxBytes int64 = 1 << 20

type PartitionLogOptions struct {
	SegmentMaxBytes int64
}

type PartitionLog struct {
	dirPath         string
	segments        []*Segment
	activeSegment   *Segment
	nextOffset      uint64
	segmentMaxBytes int64
	retentionBytes  int64
	mu              sync.Mutex
}

func OpenPartitionLog(dirpath string) (*PartitionLog, error) {
	return OpenPartitionLogWithOptions(dirpath, PartitionLogOptions{})
}

func OpenPartitionLogWithOptions(dirpath string, opts PartitionLogOptions) (*PartitionLog, error) {
	err := os.MkdirAll(dirpath, 0755)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}
	var segments []*Segment
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		baseOffsetStr := strings.TrimSuffix(name, ".log")
		baseOffset, err := strconv.ParseUint(baseOffsetStr, 10, 64)
		if err != nil {
			return nil, err
		}
		segmentPath := filepath.Join(dirpath, name)
		segment, err := OpenSegment(segmentPath, baseOffset)
		if err != nil {
			return nil, err
		}
		segments = append(segments, segment)
	}

	if len(segments) == 0 {
		firstSegmentPath := filepath.Join(dirpath, fmt.Sprintf("%020d.log", 0))
		firstSegment, err := OpenSegment(firstSegmentPath, 0)
		if err != nil {
			return nil, err
		}
		segments = append(segments, firstSegment)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].baseOffset < segments[j].baseOffset
	})

	var nextOffset uint64
	for i, segment := range segments {
		recoveredNextOffset, err := segment.RebuildIndexAndNextOffset()
		if err != nil {
			return nil, err
		}
		if i == len(segments)-1 {
			nextOffset = recoveredNextOffset
		}
	}
	activeSegment := segments[len(segments)-1]

	segmentMaxBytes := opts.SegmentMaxBytes
	if segmentMaxBytes <= 0 {
		segmentMaxBytes = defaultSegmentMaxBytes
	}

	return &PartitionLog{
		dirPath:         dirpath,
		segments:        segments,
		activeSegment:   activeSegment,
		nextOffset:      nextOffset,
		segmentMaxBytes: segmentMaxBytes,
		retentionBytes:  defaultRetentionBytes,
	}, nil
}

func (l *PartitionLog) Append(key, value []byte) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := Record{
		Offset:    l.nextOffset,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
	}
	res, err := EncodeRecord(rec)
	if err != nil {
		return Record{}, err
	}
	if len(res)+int(l.activeSegment.size) > int(l.segmentMaxBytes) {
		newSegmentPath := filepath.Join(l.dirPath, fmt.Sprintf("%020d.log", l.nextOffset))
		newSegment, err := OpenSegment(newSegmentPath, l.nextOffset)
		if err != nil {
			return Record{}, err
		}
		l.segments = append(l.segments, newSegment)
		l.activeSegment = newSegment
	}

	err = l.activeSegment.Append(rec.Offset, res)
	if err != nil {
		return Record{}, err
	}

	err = l.applyRetention()
	if err != nil {
		return Record{}, err
	}

	l.nextOffset++
	return rec, nil

}

func (l *PartitionLog) ReadAll() ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var records []Record

	for _, seg := range l.segments {
		segRecords, err := seg.ReadAll()
		if err != nil {
			return nil, err
		}

		records = append(records, segRecords...)
	}

	return records, nil
}

func (l *PartitionLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, seg := range l.segments {
		err := seg.Close()
		if err != nil {
			return err
		}

	}
	return nil
}

func (l *PartitionLog) ReadFromOffset(offset uint64) ([]Record, error) {
	return l.ReadFromOffsetN(offset, -1)
}

func (l *PartitionLog) ReadFromOffsetN(offset uint64, maxRecords int) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var records []Record
	started := false

	for i, seg := range l.segments {
		if !started {
			isLast := i == len(l.segments)-1
			if seg.baseOffset <= offset && (isLast || l.segments[i+1].baseOffset > offset) {
				remaining := remainingRecordsLimit(maxRecords, len(records))
				segRecords, err := seg.ReadFromOffsetN(offset, remaining)
				if err != nil {
					return nil, err
				}
				records = append(records, segRecords...)
				if reachedRecordsLimit(maxRecords, len(records)) {
					break
				}
				started = true
			}
			continue
		}

		remaining := remainingRecordsLimit(maxRecords, len(records))
		segRecords, err := seg.ReadAllN(remaining)
		if err != nil {
			return nil, err
		}
		records = append(records, segRecords...)
		if reachedRecordsLimit(maxRecords, len(records)) {
			break
		}
	}

	return records, nil
}

func (l *PartitionLog) totalSize() (int64, error) {
	var total int64

	for _, seg := range l.segments {
		size, err := seg.Size()
		if err != nil {
			return 0, err
		}
		total += size
	}

	return total, nil
}

func (l *PartitionLog) applyRetention() error {
	if l.retentionBytes <= 0 || len(l.segments) <= 1 {
		return nil
	}
	total, err := l.totalSize()
	if err != nil {
		return err
	}

	for total > l.retentionBytes && len(l.segments) > 1 {
		oldestSize, err := l.segments[0].Size()
		if err != nil {
			return err
		}

		if err := l.deleteOldestSegment(); err != nil {
			return err
		}

		total -= oldestSize
	}

	return nil
}

func (l *PartitionLog) deleteOldestSegment() error {
	if len(l.segments) <= 1 {
		return nil
	}

	oldest := l.segments[0]
	if err := oldest.Remove(); err != nil {
		return err
	}
	l.segments = l.segments[1:]

	return nil
}

func remainingRecordsLimit(maxRecords, current int) int {
	if maxRecords < 0 {
		return -1
	}
	if maxRecords <= current {
		return 0
	}

	return maxRecords - current
}

func reachedRecordsLimit(maxRecords, current int) bool {
	return maxRecords >= 0 && current >= maxRecords
}
