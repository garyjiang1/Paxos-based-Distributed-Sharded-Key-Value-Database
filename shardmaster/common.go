package shardmaster

//
// Master shard server: assigns Shards to replication groups.
//
// RPC interface:
// Join(GroupID, servers) -- replica group GroupID is joining, give it some Shards.
// Leave(GroupID) -- replica group GroupID is retiring, hand off all its Shards.
// Move(shard, GroupID) -- hand off one shard from current owner to GroupID.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all Shards
// assigned to group 0 (the invalid group).
//
// A GroupID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GroupID joins, and leaves, it should never join again.
//
// Please don't change this file.
//

const NShards = 10

type Config struct {
	Num    int                // config number
	Shards [NShards]int64     // GroupID
	Groups map[int64][]string // GroupID -> servers[]
}

type JoinArgs struct {
	GID     int64    // unique replica group ID
	Servers []string // group server ports
}

type JoinReply struct {
}

type LeaveArgs struct {
	GID int64
}

type LeaveReply struct {
}

type MoveArgs struct {
	Shard int
	GID   int64
}

type MoveReply struct {
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	Config Config
}
