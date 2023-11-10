package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpID  int64
	Op    string
	Key   string
	Value string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	database       sync.Map
	hashVals       sync.Map
	seq            int
	recentOps      map[int64]CachedOperation
	recentOpsQueue []int64
	maxCacheSize   int
}

type CachedOperation struct {
	OpID          int64
	PreviousValue string
}

// ExecutePaxos processes a given operation through the Paxos consensus algorithm.
func (kv *KVPaxos) ExecutePaxos(currentOp Op) bool {
	timeoutDuration := 10 * time.Millisecond

	for {
		// Check if the service instance is marked as dead.
		if kv.dead {
			return false
		}

		// Check the status of the operation with the current sequence number.
		isDecided, prevOpData := kv.px.Status(kv.seq)
		if isDecided {
			prevOp := prevOpData.(Op)
			kv.ExecuteStore(prevOp)
			kv.px.Done(kv.seq) // Mark this sequence number as decided in Paxos.
			kv.seq++
			// If the operation was the current operation, return true.
			if currentOp.OpID == prevOp.OpID {
				return true
			}
		} else {
			// Start the Paxos process with the current operation.
			kv.px.Start(kv.seq, currentOp)

			// Add jitter to the sleep duration to avoid timeout collisions
			jitter := time.Duration(rand.Int63n(10)) * time.Millisecond
			time.Sleep(timeoutDuration + jitter)

			// Exponential backoff
			timeoutDuration *= 2
			if timeoutDuration > 1*time.Second {
				timeoutDuration = 1 * time.Second
			}
		}
	}
}

// ExecuteStore processes a given operation and updates the store accordingly.
func (kv *KVPaxos) ExecuteStore(op Op) {
	if op.Op == "Put" || op.Op == "PutHash" {
		prevValue := kv.StoreValue(op.Op, op.Key, op.Value, op.OpID)
		kv.cacheOperation(op.OpID, prevValue)
	}
}

// cacheOperation adds an operation to the cache, evicting the oldest if necessary.
func (kv *KVPaxos) cacheOperation(opID int64, prevValue string) {
	// Evict the oldest entry if cache reaches max size.
	if len(kv.recentOps) >= kv.maxCacheSize {
		oldestOpID := kv.recentOpsQueue[0]
		delete(kv.recentOps, oldestOpID)
		kv.recentOpsQueue = kv.recentOpsQueue[1:]
	}

	// Cache the new operation.
	kv.recentOps[opID] = CachedOperation{
		OpID:          opID,
		PreviousValue: prevValue,
	}
	kv.recentOpsQueue = append(kv.recentOpsQueue, opID)
}

// RetrieveCachedOp fetches an operation from the cache using its ID.
func (kv *KVPaxos) RetrieveCachedOp(opID int64) (CachedOperation, bool) {
	op, found := kv.recentOps[opID]
	return op, found
}

// StoreValue stores or updates a key-value pair, possibly hashing the value.
func (kv *KVPaxos) StoreValue(action, key, value string, actionID int64) string {
	// Check if the action has been previously cached.
	cachedOp, found := kv.RetrieveCachedOp(actionID)
	// If the operation was previously cached, return its previous value.
	if found {
		return cachedOp.PreviousValue
	}

	// Initialize the previous value to an empty string.
	prevValue := ""

	// Try to load the current value associated with the key from the database.
	currentVal, exists := kv.database.Load(key)

	// If the action is "Put", store the new value into the database.
	if action == "Put" {
		kv.database.Store(key, value)
	} else {
		// If the action isn't "Put" and the key exists in the database,
		// store its current value in prevValue.
		if exists {
			prevValue = currentVal.(string)
		}

		// Check if the hash of the concatenated values of prevValue and the input value
		// for the given actionID exists.
		_, exists = kv.hashVals.Load(actionID)
		// If it doesn't exist, hash the combined values and store it in the database.
		if !exists {
			hashedVal := strconv.FormatUint(uint64(hash(prevValue+value)), 10)
			kv.database.Store(key, hashedVal)
		}
	}

	// Cache the previous value against the actionID in hashVals map.
	kv.hashVals.Store(actionID, prevValue)

	// Return the previous value.
	return prevValue
}

// RetrieveValue fetches the value associated with a given key from the database.
func (kv *KVPaxos) RetrieveValue(key string) string {
	// Attempt to load the value associated with the key from the database.
	result, exists := kv.database.Load(key)

	// If the key exists in the database, return its associated value.
	if exists {
		return result.(string)
	}

	// If the key does not exist in the database, return an empty string.
	return ""
}

// Get processes a get request, ensuring consensus via Paxos before fetching the value.
func (kv *KVPaxos) Get(query *GetArgs, resp *GetReply) error {
	// Check if the server is inactive.
	if kv.dead {
		resp.Err = "server unavailable"
		return nil
	}

	// Lock the KVPaxos instance to ensure thread safety.
	kv.mu.Lock()
	defer kv.mu.Unlock() // Unlock when the function exits.

	// Construct the operation to be processed.
	operation := Op{query.Hash, "Get", query.Key, ""}

	// Ensure consensus with Paxos before execution.
	kv.ExecutePaxos(operation)

	// Retrieve the value for the key after consensus is reached.
	resp.Value = kv.RetrieveValue(query.Key)

	return nil
}

// Put processes a put request, ensuring consensus via Paxos before updating the store.
func (kv *KVPaxos) Put(update *PutArgs, feedback *PutReply) error {
	// Check if the server is inactive.
	if kv.dead {
		feedback.Err = "server unavailable"
		return nil
	}

	// Lock the KVPaxos instance to ensure thread safety.
	kv.mu.Lock()
	defer kv.mu.Unlock() // Unlock when the function exits.

	// Construct the operation to be processed.
	operation := Op{update.Hash, update.Op, update.Key, update.Value}

	// Ensure consensus with Paxos before execution.
	kv.ExecutePaxos(operation)

	// Store the new value and get the previous value after consensus is reached.
	previousValue := kv.StoreValue(update.Op, update.Key, update.Value, update.Hash)

	// Update the feedback with the previous value.
	feedback.PreviousValue = previousValue

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.recentOps = make(map[int64]CachedOperation)
	kv.recentOpsQueue = make([]int64, 0)
	// set to potential maximum size of the cache to be 1000 for say
	kv.maxCacheSize = 1000

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
