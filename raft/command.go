package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// String renders an Operation for logging (design doc section 17's
// "command=\"PUT x 10\"" style).
func (o Operation) String() string {
	switch o {
	case PUT:
		return "PUT"
	case DELETE:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// String renders a Command for logging (design doc section 17's
// "command=\"PUT x 10\"" style).
func (c Command) String() string {
	return fmt.Sprintf("%s %s %s", c.Op, c.Key, c.Value)
}

// encodeCommand gob-encodes cmd for the wire (pb.LogEntry.Command). This is
// an internal format between our own Go processes only, and cannot
// realistically fail for a plain struct of primitives — panic rather than
// plumb an error return through every caller.
func encodeCommand(cmd Command) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		panic("encodeCommand: " + err.Error())
	}
	return buf.Bytes()
}

// decodeCommand gob-decodes data produced by encodeCommand. data is our own
// internal wire format, not an untrusted boundary, so error handling here
// stays minimal: an empty payload (e.g. a heartbeat's absent entries, or a
// zero-value Command that was never meant to carry one) decodes to the zero
// Command, and any other decode error is ignored, also yielding the zero
// Command.
func decodeCommand(data []byte) Command {
	if len(data) == 0 {
		return Command{}
	}
	var cmd Command
	_ = gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd)
	return cmd
}
