package storage

import (
	"sync"

	"github.com/colinwang05/raft-kv/raft"
)

// KVStore is the deterministic state machine produced by replaying
// committed Raft commands (design doc section 2 / "core invariant"). V1
// keeps the map purely in memory; kv.snapshot persistence is deferred.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore returns an empty KVStore.
func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]string)}
}

// Get returns the value for key, and whether it was present.
func (s *KVStore) Get(key string) (string, bool) {
	// TODO: implement.
	return "", false
}

// Put sets key to value.
func (s *KVStore) Put(key, value string) {
	// TODO: implement.
}

// Delete removes key.
func (s *KVStore) Delete(key string) {
	// TODO: implement.
}

// Apply applies one committed Command to the store. Must only be called by
// the apply loop, strictly in increasing log-index order.
func (s *KVStore) Apply(cmd raft.Command) {
	// TODO: implement (switch on cmd.Op: PUT -> Put, DELETE -> Delete).
}
