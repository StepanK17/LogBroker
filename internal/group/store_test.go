package group

import "testing"

func TestStoreCommitAndReload(t *testing.T) {
	baseDir := t.TempDir()

	store, err := OpenStore(baseDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	if err := store.Commit("group-a", "orders", 0, 42); err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	reopened, err := OpenStore(baseDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	offset, found, err := reopened.Get("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("get offset: %v", err)
	}
	if !found {
		t.Fatalf("expected committed offset to be found")
	}
	if offset != 42 {
		t.Fatalf("expected offset 42, got %d", offset)
	}
}

func TestStoreGetMissingOffset(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	offset, found, err := store.Get("missing-group", "orders", 0)
	if err != nil {
		t.Fatalf("get offset: %v", err)
	}
	if found {
		t.Fatalf("expected offset to be missing, got %d", offset)
	}
}
