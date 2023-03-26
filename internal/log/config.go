package log

import (
	"github.com/hashicorp/raft"
)

type Config struct {
	Raft struct {
		raft.Config
		StreamLayer *StreamLayer
		// Bootstrap is whether it is an initial node that needs to bootstrapp nor not.
		// After it bootstrapps, it will be the first leader and add other servers (nodes) that won't bootstrapp later.
		Bootstrap bool
	}
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
