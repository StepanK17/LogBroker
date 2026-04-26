package group

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type Store struct {
	path    string
	offsets map[string]uint64
	mu      sync.RWMutex
}

func OpenStore(baseDir string) (*Store, error) {
	path := filepath.Join(baseDir, "groups", "offsets.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	store := &Store{
		path:    path,
		offsets: make(map[string]uint64),
	}

	if err := store.load(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *Store) Commit(group, topic string, partition int, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := makeKey(group, topic, partition)
	s.offsets[key] = offset
	if err := s.save(); err != nil {
		delete(s.offsets, key)
		return err
	}
	return nil
}

func (s *Store) Get(group, topic string, partition int) (uint64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offset, ok := s.offsets[makeKey(group, topic, partition)]
	if !ok {
		return 0, false, nil
	}
	return offset, true, nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if len(data) == 0 {
		return nil
	}

	return json.Unmarshal(data, &s.offsets)
}

func (s *Store) save() error {
	data, err := json.MarshalIndent(s.offsets, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}

	return os.Rename(tmpPath, s.path)
}

func makeKey(group, topic string, partition int) string {
	return filepath.Join(group, topic, strconv.Itoa(partition))
}
