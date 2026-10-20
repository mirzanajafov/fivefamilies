package server

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type Config struct {
	ID            raft.NodeID
	Nodes         map[raft.NodeID]string
	Voters        []raft.NodeID
	DataDir       string
	Tick          time.Duration
	HeartbeatTick int
	ElectionTick  int
	LeaseTicks    int
	SnapshotEvery uint64
	Retain        uint64
	SegmentBytes  int64
	MaxSessions   int
	WatchHistory  int
	WatchProgress time.Duration
	Bugs          raft.Bug
	Logger        *slog.Logger

	FsyncMode       string
	SyncStorage     bool
	WriteBeforeSend bool
	MaxInflight     int
	MaxMsgBytes     int
	MaxPending      uint64
	Batch           int
	Chaos           bool
}

const (
	FsyncGroup = "group"
	FsyncReady = "ready"
	FsyncEntry = "entry"
	FsyncOff   = "off"
)

func (c *Config) defaults() error {
	if c.ID == raft.None || c.Nodes[c.ID] == "" {
		return errors.New("server: the node's own ID is not in the cluster")
	}
	if len(c.Voters) == 0 {
		for id := range c.Nodes {
			c.Voters = append(c.Voters, id)
		}
	}
	slices.Sort(c.Voters)
	for _, id := range c.Voters {
		if c.Nodes[id] == "" {
			return fmt.Errorf("server: voter %d has no address", id)
		}
	}
	if c.Tick <= 0 {
		c.Tick = 10 * time.Millisecond
	}
	if c.HeartbeatTick <= 0 {
		c.HeartbeatTick = 10
	}
	if c.ElectionTick <= 0 {
		c.ElectionTick = 10 * c.HeartbeatTick
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	switch c.FsyncMode {
	case "":
		c.FsyncMode = FsyncGroup
	case FsyncGroup, FsyncReady, FsyncEntry, FsyncOff:
	default:
		return fmt.Errorf("server: unknown fsync mode %q", c.FsyncMode)
	}
	if c.WatchProgress <= 0 {
		c.WatchProgress = time.Second
	}
	if c.Batch <= 0 {
		c.Batch = 256
	}
	return nil
}
