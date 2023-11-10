package kvpaxos

import "net/rpc"
import "fmt"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	// Initialize request arguments
	args := &GetArgs{Key: key, Hash: nrand()}
	reply := &GetReply{}

	// Start with the first server and loop through all servers until a successful call
	for i := 0; !call(ck.servers[i], "KVPaxos.Get", args, reply); {
		// Cycle to the next server
		i = (i + 1) % len(ck.servers)
		// Gradually increase delay between retries
		time.Sleep(time.Millisecond * time.Duration(50*i))
	}

	// Return the retrieved value
	return reply.Value
}

// set the value for a key.
// keeps trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// Decide operation type based on the dohash flag
	opType := "Put"
	if dohash {
		opType = "PutHash"
	}

	// Initialize request arguments
	args := &PutArgs{Key: key, Value: value, Op: opType, Hash: nrand()}
	reply := &PutReply{}

	// Start with the first server and loop through all servers until a successful call
	for i := 0; !call(ck.servers[i], "KVPaxos.Put", args, reply); {
		// Cycle to the next server
		i = (i + 1) % len(ck.servers)
		// Gradually increase delay between retries
		time.Sleep(time.Millisecond * time.Duration(50*i))
	}

	// Return the previous value if hashed put was performed, else return an empty string
	if dohash {
		return reply.PreviousValue
	}
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
