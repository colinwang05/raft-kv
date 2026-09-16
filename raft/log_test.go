package raft

import "testing"

func TestLastLogIndexAndTerm(t *testing.T) {
	n := &Node{}
	if idx := n.lastLogIndex(); idx != 0 {
		t.Errorf("empty log: lastLogIndex() = %d, want 0", idx)
	}
	if term := n.lastLogTerm(); term != 0 {
		t.Errorf("empty log: lastLogTerm() = %d, want 0", term)
	}

	n.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	}
	if idx := n.lastLogIndex(); idx != 3 {
		t.Errorf("lastLogIndex() = %d, want 3", idx)
	}
	if term := n.lastLogTerm(); term != 2 {
		t.Errorf("lastLogTerm() = %d, want 2", term)
	}
}

func TestIsLogUpToDate(t *testing.T) {
	tests := []struct {
		name                                  string
		ourLog                                []LogEntry
		candidateLastIndex, candidateLastTerm uint64
		want                                  bool
	}{
		{
			name: "both empty logs are equally up to date",
			want: true,
		},
		{
			name:               "candidate has higher last-log term",
			ourLog:             []LogEntry{{Index: 5, Term: 3}},
			candidateLastIndex: 1,
			candidateLastTerm:  4,
			want:               true, // higher term wins outright, even with a shorter log
		},
		{
			name:               "candidate has lower last-log term",
			ourLog:             []LogEntry{{Index: 1, Term: 4}},
			candidateLastIndex: 5,
			candidateLastTerm:  3,
			want:               false, // lower term loses outright, even with a longer log
		},
		{
			name:               "same term, candidate log at least as long",
			ourLog:             []LogEntry{{Index: 3, Term: 2}},
			candidateLastIndex: 3,
			candidateLastTerm:  2,
			want:               true,
		},
		{
			name:               "same term, candidate log longer",
			ourLog:             []LogEntry{{Index: 3, Term: 2}},
			candidateLastIndex: 5,
			candidateLastTerm:  2,
			want:               true,
		},
		{
			name:               "same term, candidate log shorter",
			ourLog:             []LogEntry{{Index: 5, Term: 2}},
			candidateLastIndex: 3,
			candidateLastTerm:  2,
			want:               false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &Node{log: tt.ourLog}
			got := n.isLogUpToDate(tt.candidateLastIndex, tt.candidateLastTerm)
			if got != tt.want {
				t.Errorf("isLogUpToDate(%d, %d) = %v, want %v",
					tt.candidateLastIndex, tt.candidateLastTerm, got, tt.want)
			}
		})
	}
}
