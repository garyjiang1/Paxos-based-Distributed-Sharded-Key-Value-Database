package shardmaster

import (
	"container/list"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sort"
	"sync"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configurations []Config // Historical configurations, indexed by their number
	highestConfig  int      // The highest configuration number achieved
	paxosSeq       int      // Sequence number for Paxos operations
	logger         *log.Logger
}

type Op struct {
	// Your data here.
	Type       string   // Operation type: Join, Leave, Move, Query
	GroupID    int64    // Group ID for the operation
	Servers    []string // Server list for Join operations
	ShardIndex int      // Shard index for Move operations
	QueryID    int      // Identifier for Query operations
}

type ShardGroup struct {
	GroupID int64
	Shards  *list.List
}

type ShardDistribution struct {
	ShardsPerGroup map[int64]*ShardGroup // Mapping from GroupID to ShardGroup
	LastShards     *list.List            // List of last Shards, used in distribution
}

type ShardBalanceMetrics struct {
	MinShards int // Minimum number of Shards per group
	MaxShards int // Maximum number of Shards per group
}

/*
Idea breakdown:
Initial ShardIndex Assignment:
Shards are assigned to groups based on their current group in conf.Shards. If a shard does not belong to any existing group, it is added to the last group encountered. This step ensures that all Shards are initially allocated to some group.

Sorting Groups by ShardIndex Count:
After the initial assignment, all groups are organized into a slice. This slice is then sorted based on the number of Shards in each group. Sorting groups by shard count allows for easy identification of which groups have the most and fewest Shards, which is critical for the subsequent rebalancing process.

Simplified Balancing Logic:
The balancing process involves iteratively moving a single shard at a time from the group with the most Shards to the group with the fewest. The group with the most Shards is always at the end of the sorted slice, while the group with the fewest is at the beginning. This process continues until all groups have a shard count within the defined minimum and maximum range.
Consistent Re-sorting: Each time a shard is moved, the groups are re-sorted to ensure that the order (from fewest to most Shards) is maintained. This consistent re-sorting is crucial as it ensures that in the next iteration, the shard movement will again occur from the group with the most Shards to the group with the least.
Final Configuration Update: Once the shard distribution is balanced within the acceptable range, the conf.Shards array is updated to reflect the new distribution. This update is essential to ensure that the configuration accurately represents the current state of shard allocation across the groups.
*/
func (sm *ShardMaster) balance(conf *Config) {
	// Initialize shard groups and prepare the last group for new shards if needed.
	distribution := sm.initializeShardGroups(conf)

	// Distribute shards across the initialized groups.
	sm.distributeShards(conf, distribution.ShardsPerGroup, distribution.LastShards)

	// Calculate the minimum and maximum number of shards that each group should ideally have.
	metrics := sm.calculateShardLimits(len(conf.Groups))

	// Sort the groups by the current number of shards they hold, in ascending order.
	sortedGroups := sm.sortGroupsByShardCount(distribution.ShardsPerGroup)

	// Rebalance the shards among the groups to ensure an even distribution.
	sm.rebalancedShards(metrics.MinShards, metrics.MaxShards, sortedGroups, conf)
}

// initializeShardGroups sets up a mapping of groups to their associated shard lists.
func (sm *ShardMaster) initializeShardGroups(conf *Config) ShardDistribution {
	shardsPerGroup := make(map[int64]*ShardGroup)
	var lastShards *list.List

	// Create a new ShardGroup for each group in the configuration.
	for gid := range conf.Groups {
		shardsPerGroup[gid] = &ShardGroup{GroupID: gid, Shards: new(list.List)}
		lastShards = shardsPerGroup[gid].Shards
	}

	return ShardDistribution{ShardsPerGroup: shardsPerGroup, LastShards: lastShards}
}

// distributeShards allocates Shards to their respective groups as per the configuration.
func (sm *ShardMaster) distributeShards(conf *Config, shardsPerGroup map[int64]*ShardGroup, lastShards *list.List) {
	// Assign each shard to its group, or to the last group if not already assigned.
	for i, v := range conf.Shards {
		if group, ok := shardsPerGroup[v]; ok {
			group.Shards.PushBack(i)
		} else {
			lastShards.PushBack(i)
		}
	}
}

// calculateShardLimits computes the minimum and maximum number of Shards per group.
func (sm *ShardMaster) calculateShardLimits(groupNum int) ShardBalanceMetrics {
	minShards := NShards / groupNum
	maxShards := minShards

	// If shards don't divide evenly among groups, increase the maxShards limit by 1.
	if NShards%groupNum != 0 {
		maxShards++
	}

	return ShardBalanceMetrics{MinShards: minShards, MaxShards: maxShards}
}

// sortGroupsByShardCount organizes the groups based on the number of Shards they contain.
func (sm *ShardMaster) sortGroupsByShardCount(shardsPerGroup map[int64]*ShardGroup) []ShardGroup {
	sortedGroups := make([]ShardGroup, 0, len(shardsPerGroup))

	// Populate the slice with each group's ID and their corresponding shard list.
	for gid, group := range shardsPerGroup {
		sortedGroups = append(sortedGroups, ShardGroup{gid, group.Shards})
	}

	// Sort the groups in ascending order of the number of shards they hold.
	sort.Slice(sortedGroups, func(i, j int) bool {
		return sortedGroups[i].Shards.Len() < sortedGroups[j].Shards.Len()
	})

	return sortedGroups
}

// rebalancedShards redistributes Shards between groups to achieve a more balanced shard distribution.
func (sm *ShardMaster) rebalancedShards(minShards, maxShards int, sortedGroups []ShardGroup, conf *Config) {
	// Loop until all groups have Shards within the min and max limits.
	for sortedGroups[0].Shards.Len() < minShards || sortedGroups[len(sortedGroups)-1].Shards.Len() > maxShards {
		// Move a shard from the group with the most Shards to the group with the least.
		shardToMove := sortedGroups[len(sortedGroups)-1].Shards.Front()
		sortedGroups[len(sortedGroups)-1].Shards.Remove(shardToMove)
		sortedGroups[0].Shards.PushBack(shardToMove.Value)

		// Re-sort the groups after each move to maintain the order.
		sort.Slice(sortedGroups, func(i, j int) bool {
			return sortedGroups[i].Shards.Len() < sortedGroups[j].Shards.Len()
		})
	}

	// Update the configuration with the new shard assignments.
	for _, group := range sortedGroups {
		for e := group.Shards.Front(); e != nil; e = e.Next() {
			conf.Shards[e.Value.(int)] = group.GroupID
		}
	}
}

/*
Idea Breakdown for ShardMaster's Request Handling:

1. Operation Initialization:
  - The process starts with initializing the operation based on its type ('Join', 'Leave', 'Move', 'Query') and arguments.
  - Each operation type is handled separately, with specific logic and error handling for invalid arguments or types.

2. Executing Paxos Workflow:
  - Once the operation is initialized, the Paxos consensus workflow is executed.
  - This involves starting the Paxos process and repeatedly checking if consensus has been reached, with a pause between each check.

3. Paxos Process Start and Status Check:
  - The Paxos process begins for a specific operation, and its status is continuously monitored.
  - If a decision is made, the operation result is processed; otherwise, the process keeps waiting for a consensus.

4. Operation Matching and Result Processing:
  - After reaching a consensus, the code verifies if the operation decided by Paxos matches the intended operation.
  - The results of the operation are then used to update the system's configuration, adapting to the new state determined by the consensus.

5. Configuration Preparation and Update:
  - A new configuration is prepared based on the last known configuration, incrementing its number and copying relevant settings.
  - The changes dictated by the operation result are applied to this new configuration.

6. Final Configuration and Paxos Sequence Completion:
  - The newly updated configuration is finalized and added to the system's configuration history.
  - The Paxos sequence is marked as completed, signaling the end of the consensus process for the current operation.
*/
func (sm *ShardMaster) request(optype string, args interface{}) error {
	// sm.logger.Printf("Received request: %s", optype)

	op, err := sm.initializeOperation(optype, args)
	if err != nil {
		// sm.logger.Printf("Error initializing operation: %s, Error: %v", optype, err)
		return err
	}

	sm.executePaxosWorkflow(&op)
	sm.finalizePaxosSequence()

	// sm.logger.Printf("Completed request: %s", optype)
	return nil
}

// initializeOperation creates and initializes an operation based on the provided type and arguments.
// It supports various operation types like 'Join', 'Leave', 'Move', and 'Query'. Each operation type
// is processed using specific helper functions. In case of an invalid operation type or error in
// processing arguments, it returns an error.
func (sm *ShardMaster) initializeOperation(optype string, args interface{}) (Op, error) {
	var op Op
	var err error

	switch optype {
	case "Join":
		op, err = sm.prepareJoinOperation(args)
		if err != nil {
			return Op{}, err
		}
	case "Leave":
		op, err = sm.prepareLeaveOperation(args)
		if err != nil {
			return Op{}, err
		}
	case "Move":
		op, err = sm.prepareMoveOperation(args)
		if err != nil {
			return Op{}, err
		}
	case "Query":
		op = Op{Type: optype, QueryID: sm.me}
	default:
		return Op{}, fmt.Errorf("invalid operation type: %s", optype)
	}
	if err != nil {
		// sm.logger.Printf("Error in initializeOperation for %s: %v", optype, err)
	}
	return op, err
}

// prepareJoinOperation initializes a 'Join' operation. It asserts the type of the provided arguments
// and returns an error if they are not of the expected type. On successful type assertion, it returns
// a 'Join' operation with the appropriate fields set.
func (sm *ShardMaster) prepareJoinOperation(args interface{}) (Op, error) {
	joinArgs, ok := args.(JoinArgs)
	if !ok {
		return Op{}, fmt.Errorf("invalid args for Join operation")
	}
	return Op{Type: "Join", GroupID: joinArgs.GID, Servers: joinArgs.Servers}, nil
}

// prepareLeaveOperation initializes a 'Leave' operation by setting up the required fields
// based on the provided arguments.
func (sm *ShardMaster) prepareLeaveOperation(args interface{}) (Op, error) {
	leaveArgs := args.(LeaveArgs)
	return Op{Type: "Leave", GroupID: leaveArgs.GID}, nil
}

// prepareMoveOperation initializes a 'Move' operation. It constructs the operation with the
// necessary data extracted from the provided arguments.
func (sm *ShardMaster) prepareMoveOperation(args interface{}) (Op, error) {
	moveArgs := args.(MoveArgs)
	return Op{Type: "Move", GroupID: moveArgs.GID, ShardIndex: moveArgs.Shard}, nil
}

// executePaxosWorkflow manages the Paxos consensus process for the given operation.
// It repeatedly checks if consensus has been reached and waits for a brief period between checks.
func (sm *ShardMaster) executePaxosWorkflow(op *Op) {
	// sm.logger.Printf("Starting Paxos workflow for operation: %+v", *op)
	for !sm.consensusReached(op) {
		sm.startPaxosProcess(op)
		// sm.logger.Printf("Waiting for consensus on operation: %+v", *op)
		time.Sleep(10 * time.Millisecond)
	}
	// sm.logger.Printf("Consensus reached for operation: %+v", *op)
}

// startPaxosProcess begins the Paxos consensus process for a specific operation.
// It checks the Paxos status and processes the operation result if a decision has been made.
func (sm *ShardMaster) startPaxosProcess(op *Op) {
	sm.px.Start(sm.paxosSeq, *op)
	decided, val := sm.px.Status(sm.paxosSeq)
	if decided {
		sm.processOperationResult(val, op)
		sm.paxosSeq++
	}
}

// consensusReached determines whether consensus has been reached in the Paxos process
// for the provided operation.
func (sm *ShardMaster) consensusReached(op *Op) bool {
	decided, val := sm.px.Status(sm.paxosSeq)

	// Log the status check
	// sm.logger.Printf("Checking if consensus is reached for Paxos Seq: %d", sm.paxosSeq)

	if !decided {
		// sm.logger.Printf("Consensus not yet reached for Paxos Seq: %d", sm.paxosSeq)
		return false
	}

	decode, ok := val.(Op)
	if !ok {
		// Log fatal error and exit
		// sm.logger.Fatalf("Invalid operation type from Paxos status for Seq: %d", sm.paxosSeq)
	}

	isMatch := sm.isOperationMatching(decode, *op)
	// sm.logger.Printf("Operation match status for Paxos Seq: %d is %v", sm.paxosSeq, isMatch)

	return isMatch
}

// processOperationResult handles the outcome of an operation post consensus. It applies
// the necessary changes to the system's configuration based on the operation result.
func (sm *ShardMaster) processOperationResult(val interface{}, op *Op) {
	decode, _ := val.(Op)
	newConfig := sm.prepareNewConfig()
	sm.applyOperationChanges(decode, &newConfig)
	sm.finalizeNewConfig(newConfig)
}

// isOperationMatching checks if the operation decided by Paxos matches the intended operation.
func (sm *ShardMaster) isOperationMatching(decode Op, op Op) bool {
	return decode.Type == op.Type && decode.GroupID == op.GroupID && decode.ShardIndex == op.ShardIndex
}

// applyOperationChanges applies changes to the system configuration based on the operation type.
// It handles 'Join', 'Leave', and 'Move' operations by updating the configuration accordingly.
func (sm *ShardMaster) applyOperationChanges(decode Op, newConfig *Config) {
	switch decode.Type {
	case "Join":
		newConfig.Groups[decode.GroupID] = decode.Servers
		sm.balance(newConfig)
	case "Leave":
		delete(newConfig.Groups, decode.GroupID)
		sm.balance(newConfig)
	case "Move":
		newConfig.Shards[decode.ShardIndex] = decode.GroupID
	}
}

// prepareNewConfig prepares a new configuration by incrementing the config number and copying
// the previous settings, ensuring that changes do not affect older configurations.
func (sm *ShardMaster) prepareNewConfig() Config {
	lastConfig := sm.configurations[sm.highestConfig]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: deepCopyGroups(lastConfig.Groups),
	}
	return newConfig
}

// deepCopyGroups creates a deep copy of the group data. This function is used in prepareNewConfig
// to ensure that modifications to new configurations do not impact existing ones.
func deepCopyGroups(groups map[int64][]string) map[int64][]string {
	newGroups := make(map[int64][]string)
	for k, v := range groups {
		newGroups[k] = append([]string(nil), v...)
	}
	return newGroups
}

// finalizeNewConfig finalizes the updated configuration, appending it to the system's configuration
// history and ensuring consistency in configuration numbering.
func (sm *ShardMaster) finalizeNewConfig(newConfig Config) {
	sm.configurations = append(sm.configurations, newConfig)
	sm.highestConfig++
	if sm.configurations[newConfig.Num].Num != newConfig.Num {
		// sm.logger.Printf("New configuration finalized: %+v", newConfig)
	}
}

// finalizePaxosSequence marks the completion of the Paxos consensus process for the current operation.
func (sm *ShardMaster) finalizePaxosSequence() {
	sm.px.Done(sm.paxosSeq - 1)
	// sm.logger.Printf("Paxos sequence finalized: %d", sm.paxosSeq)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	err := sm.request("Join", *args)
	if err != nil {
		return err
	}

	// Uncomment for debugging
	// DPrintfCLR(1, "[Join] args=%+v\n%+v\n", *args, sm.configurations[sm.highestConfig])

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	err := sm.request("Leave", *args)
	if err != nil {
		return err
	}

	// Uncomment for debugging
	// DPrintfCLR(2, "[Leave] args=%+v\n%+v\n", *args, sm.configurations[sm.highestConfig])

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	err := sm.request("Move", *args)
	if err != nil {
		return err
	}

	// Uncomment for debugging
	// DPrintfCLR(3, "[Move] args=%+v\n%+v\n", *args, sm.configurations[sm.highestConfig])

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	num := args.Num
	if num == -1 || num >= sm.highestConfig {
		err := sm.request("Query", *args)
		if err != nil {
			return err
		}
		reply.Config = sm.configurations[sm.highestConfig]
	} else {
		reply.Config = sm.configurations[num]
	}

	if num != reply.Config.Num && num != -1 {
		// return fmt.Errorf("query reply wrong! Requested num: %d, but got: %d", num, reply.Config.Num)
	}

	// Uncomment or for debugging
	// DPrintfCLR(4, "[Query] args=%+v\n%+v\n", *args, reply)

	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configurations = make([]Config, 1)
	sm.configurations[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)
	sm.highestConfig = 0
	sm.paxosSeq = 0
	sm.logger = log.New(os.Stdout, "ShardMaster: ", log.LstdFlags)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
