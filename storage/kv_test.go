package storage

import (
	"testing"

	"github.com/colinwang05/raft-kv/raft"
)

func TestPutThenGetReturnsValue(t *testing.T) {
	s := NewKVStore()
	s.Put("k", "v")

	value, ok := s.Get("k")
	if !ok {
		t.Fatal("Get ok = false, want true after Put")
	}
	if value != "v" {
		t.Errorf("Get value = %q, want %q", value, "v")
	}
}

func TestGetMissingKeyReturnsFalse(t *testing.T) {
	s := NewKVStore()

	value, ok := s.Get("missing")
	if ok {
		t.Error("Get ok = true, want false for a missing key")
	}
	if value != "" {
		t.Errorf("Get value = %q, want \"\"", value)
	}
}

func TestDeleteRemovesKey(t *testing.T) {
	s := NewKVStore()
	s.Put("k", "v")
	s.Delete("k")

	if _, ok := s.Get("k"); ok {
		t.Error("Get ok = true after Delete, want false")
	}
}

func TestApplyDispatchesPutAndDelete(t *testing.T) {
	s := NewKVStore()

	s.Apply(raft.Command{Op: raft.PUT, Key: "k", Value: "v"})
	value, ok := s.Get("k")
	if !ok || value != "v" {
		t.Fatalf("after Apply(PUT), Get() = (%q, %v), want (%q, true)", value, ok, "v")
	}

	s.Apply(raft.Command{Op: raft.DELETE, Key: "k"})
	if _, ok := s.Get("k"); ok {
		t.Error("after Apply(DELETE), Get ok = true, want false")
	}
}
