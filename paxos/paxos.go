package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Err string

// BaseArgs Base structs
type BaseArgs struct {
	SequenceNumber int // A sequence or ordering number, often used for ordering proposals or operations
	ProposalID     int // A unique identifier for a given proposal or request
}

type BaseReply struct {
	Err Err
	N   int
}

type State struct {
	Status              string      // Describes the current state or outcome of a proposal/decision
	ProposalNumber      int         // The number of the current proposal
	AcceptedProposalNum int         // The number of the highest accepted proposal
	AcceptedValue       interface{} // The value associated with the highest accepted proposal
}

type PrepareArgs struct {
	BaseArgs
	InitiatorID   int
	LatestDoneSeq int
}

type AcceptArgs struct {
	BaseArgs
	AcceptedValue interface{}
}

type DecidedArgs struct {
	BaseArgs
	DecidedValue interface{}
}

type PrepareReply struct {
	BaseReply
	HighestAcceptedID     int
	HighestAcceptedValue  interface{}
	AuxiliaryValue        int
	MaxObservedProposalID int
	CurrentDone           int
}

type AcceptReply struct {
	BaseReply
	AuxiliaryValue int
}

type DecidedReply struct {
	BaseReply
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	state           State          // Represents the current state or status of the Paxos instance
	instances       sync.Map       // Concurrent map to store Paxos instances or agreements, indexed by sequence numbers
	doneValues      sync.Map       // Concurrent map to store completed or 'done' sequence numbers for each Paxos instance
	waitGroup       sync.WaitGroup // Used to synchronize and ensure that all goroutines have finished their tasks before moving forward
	proposalCounter int            // Counter for tracking the number of proposals or rounds initiated by this Paxos instance
	peerDone        []int          // Slice to track the highest sequence number known to be done by each peer
}

// delayRandomDuration introduces a random sleep to delay actions, which can help with randomness in Paxos proposals.
func delayRandomDuration() {
	duration := time.Duration(rand.Intn(50)) * time.Millisecond
	time.Sleep(duration)
}

// majorityCount computes the number of peers required to achieve a majority in the Paxos protocol.
func majorityCount(peers []string) int {
	return (len(peers) + 1) / 2
}

// dispatchToPeers executes the specified action concurrently for each peer.
func (px *Paxos) dispatchToPeers(action func(int, string)) {
	localWG := &sync.WaitGroup{}
	for idx, peerAddr := range px.peers {
		localWG.Add(1)
		go func(peerIdx int, address string) {
			defer localWG.Done()
			action(peerIdx, address)
		}(idx, peerAddr)
	}
	localWG.Wait()
}

// remoteInvoke conditionally makes a remote procedure call or invokes a local function based on the 'invokeSelf' flag.
func (px *Paxos) remoteInvoke(target, method string, request, response interface{}, invokeSelf bool, localFunction func(args interface{}, resp interface{})) bool {
	if invokeSelf {
		localFunction(request, response)
		return true
	}
	return call(target, method, request, response)
}

// handleAcceptorPrepare processes the Acceptor's Prepare phase.
func (px *Paxos) handleAcceptorPrepare(request interface{}, response interface{}) {
	// Call the RPCPrepare method and handle any errors.
	if err := px.RPCPrepare(request.(*PrepareArgs), response.(*PrepareReply)); err != nil {
		return
	}
}

// handleAcceptorAccept processes the Acceptor's Accept phase.
func (px *Paxos) handleAcceptorAccept(request interface{}, response interface{}) {
	// Call the RPCAccept method and handle any errors.
	if err := px.RPCAccept(request.(*AcceptArgs), response.(*AcceptReply)); err != nil {
		return
	}
}

// handleAcceptorDecided checks if the Acceptor has made a decision.
func (px *Paxos) handleAcceptorDecided(request interface{}, response interface{}) {
	// Call the RPCDecide method and handle any errors.
	if err := px.RPCDecide(request.(*DecidedArgs), response.(*DecidedReply)); err != nil {
		return
	}
}

// adjustDoneValue updates the 'done' value for a given peer if the new value is greater than the existing one.
func (px *Paxos) adjustDoneValue(newVal int, idx int) {
	if existingValue, found := px.doneValues.Load(idx); !found {
		px.doneValues.Store(idx, newVal)
	} else {
		currentValue := existingValue.(int)
		if newVal > currentValue {
			px.doneValues.Store(idx, newVal)
		}
	}
}

// getStateOrCreate retrieves or creates a state for a given sequence number.
func (px *Paxos) getStateOrCreate(seqNum int) *State {
	stateData, _ := px.instances.LoadOrStore(seqNum, &State{Status: "Pending", ProposalNumber: -1, AcceptedProposalNum: -1, AcceptedValue: nil})
	return stateData.(*State)
}

// storeInstanceState saves the state for a given sequence number.
func (px *Paxos) storeInstanceState(seq int, state *State) {
	px.instances.Store(seq, state)
}

// loadInstanceState fetches the state for a specified sequence number.
func (px *Paxos) loadInstanceState(seq int) *State {
	stateInterface, ok := px.instances.Load(seq)
	if !ok {
		return nil
	}
	return stateInterface.(*State)
}

// initializeDoneValues sets initial 'done' values for all peers.
func (px *Paxos) initializeDoneValues() {
	for i := range px.peers {
		px.doneValues.Store(i, -1)
	}
}

// loadDoneValue fetches the 'done' value for the current Paxos instance.
func (px *Paxos) loadDoneValue() int {
	doneValue, _ := px.doneValues.Load(px.me)
	return doneValue.(int)
}

// launchProposal starts a proposal if the sequence number is valid.
func (px *Paxos) launchProposal(seq int, v interface{}) {
	if seq >= px.Min() {
		px.launch(seq, v)
	}
}

// updateProposalNum generates a new proposal number by incrementing the highest known proposal number.
func updateProposalNum(maxProposalNum int) int {
	return maxProposalNum + 1
}

// isProposalRankHigher checks if the given proposalID is higher than or equal to the existing proposal number.
func (px *Paxos) isProposalRankHigher(proposalID int, existingProposalNumber int) bool {
	return proposalID >= existingProposalNumber
}

// garbageCleaning repeatedly calls the Done method to perform garbage collection.
func (px *Paxos) garbageCleaning() {
	for !px.dead {
		px.Done(-1)
		time.Sleep(time.Millisecond * 50)
	}
}

// updateDoneValues updates the 'done' values for a specific peer.
func (px *Paxos) updateDoneValues(initiatorID int, latestDoneSeq int) {
	value, _ := px.doneValues.Load(initiatorID)
	if latestDoneSeq > value.(int) {
		px.doneValues.Store(initiatorID, latestDoneSeq)
	}
}

// getCurrentDoneValue fetches the 'done' value for the current peer.
func (px *Paxos) getCurrentDoneValue() int {
	currentDone, _ := px.doneValues.Load(px.me)
	return currentDone.(int)
}

// updateCurrentState sets the current state's proposal number and status.
func (px *Paxos) updateCurrentState(state *State, proposalID int) {
	if state.Status != "Decided" {
		state.Status = "Pending"
	}
	state.ProposalNumber = proposalID
}

// setReplyFromState fills a PrepareReply with the appropriate details from a given state.
func (px *Paxos) setReplyFromState(state *State, reply *PrepareReply) {
	reply.AuxiliaryValue = px.getCurrentDoneValue()
	reply.HighestAcceptedID = state.AcceptedProposalNum
	reply.HighestAcceptedValue = state.AcceptedValue
}

// retrieveState returns the current state for the given sequence number.
// If no state is found, it will create and return a new state.
func (px *Paxos) retrieveState(seqNum int) *State {
	return px.getStateOrCreate(seqNum)
}

// isStateDecided checks if the provided state represents a decided value in the Paxos protocol.
func (px *Paxos) isStateDecided(state *State) bool {
	return state.Status == "Decided"
}

// markStateAsDecided updates the provided state to reflect that a value has been decided upon in Paxos.
func (px *Paxos) markStateAsDecided(state *State, args *DecidedArgs) {
	state.Status = "Decided"
	state.ProposalNumber = args.ProposalID
	state.AcceptedProposalNum = args.ProposalID
	state.AcceptedValue = args.DecidedValue
}

// updateStateWithAcceptedProposal updates the Paxos instance state with the accepted proposal details.
func (px *Paxos) updateStateWithAcceptedProposal(state *State, args *AcceptArgs) {
	// Update the proposal number and accepted proposal number.
	state.ProposalNumber = args.ProposalID
	state.AcceptedProposalNum = args.ProposalID

	// Update the accepted value with the value from the accepted proposal.
	state.AcceptedValue = args.AcceptedValue
}

// launch initiates the Paxos consensus process for the given sequence number and value.
// It continually tries to propose the value until it's accepted by a majority of the peers.
func (px *Paxos) launch(seq int, v interface{}) {
	// Initialize proposal state
	isDecided := false
	proposalNum := seq + px.me
	proposalStartTime := time.Now()

	//log.Printf("launch: Starting proposal with seq: %d and value: %v", seq, v)
	// Continue proposing until the value is decided
	for !isDecided {
		// If proposal has been running too long, check if instance is dead
		if time.Since(proposalStartTime) > 50*time.Millisecond {
			if px.dead {
				//log.Printf("launch: Instance is dead. Exiting.")
				return
			}
			proposalStartTime = time.Now()
		}

		//log.Printf("launch: Calling runPrepare with proposalNum: %d", proposalNum)
		// Begin the Prepare phase of Paxos
		gotMajority, proposedVal, maxProposalNum := px.runPrepare(proposalNum, seq, v)

		// Check result of Prepare phase
		if !gotMajority {
			//log.Printf("launch: Didn't get majority in runPrepare phase. maxProposalNum: %d", maxProposalNum)
			proposalNum = updateProposalNum(maxProposalNum)
			delayRandomDuration()
			continue
		}

		//log.Printf("launch: Calling runAccept with proposalNum: %d and proposedVal: %v", proposalNum, proposedVal)
		// Begin the Accept phase of Paxos
		if !px.runAccept(proposalNum, seq, proposedVal) {
			//log.Printf("launch: Accept phase failed.")
			delayRandomDuration()
			continue
		}

		//log.Printf("launch: Calling runDecide with proposalNum: %d", proposalNum)
		// Begin the Decide phase of Paxos
		isDecided = px.runDecide(proposalNum, seq, proposedVal)
		if !isDecided {
			//log.Printf("launch: Value not decided yet. Retrying...")
			delayRandomDuration()
		} else {
			//log.Printf("launch: Value decided.")
		}
	}
}

// runPrepare is the runPrepare phase of the Paxos algorithm.
// It sends runPrepare requests to all peers and aggregates their responses to determine
// whether to proceed to the accept phase also have many test partition error
func (px *Paxos) runPrepare(N int, seq int, v interface{}) (bool, interface{}, int) {
	// Initial values for the number of affirmative responses,
	// highest previously accepted proposal, and the maximum observed proposal.
	affirmativeResponses, highestPrevAcceptedProposal, maxObservedProposal := 0, -1, N
	var valueOfHighestPrevProposal interface{}

	// Dispatch requests to all peers.
	px.dispatchToPeers(func(i int, peer string) {
		// Prepare the arguments for the RPCPrepare RPC.
		args := &PrepareArgs{
			BaseArgs: BaseArgs{SequenceNumber: seq, ProposalID: N},
		}
		var reply PrepareReply

		// Invoke the RPCPrepare RPC on the peer.
		ok := px.remoteInvoke(peer, "Paxos.RPCPrepare", args, &reply, i == px.me, px.handleAcceptorPrepare)

		// If the RPC was successful and there's no error in the reply.
		if ok && reply.Err == "" {
			px.mu.Lock() // Lock to ensure thread-safety when updating shared variables.

			// Increment the count of affirmative responses.
			affirmativeResponses++

			// Update the highest accepted proposal details, if this reply has a higher proposal ID.
			if reply.HighestAcceptedID > highestPrevAcceptedProposal {
				highestPrevAcceptedProposal = reply.HighestAcceptedID
				valueOfHighestPrevProposal = reply.HighestAcceptedValue
			}

			// Adjust the 'done' value for this peer.
			px.adjustDoneValue(reply.AuxiliaryValue, i)

			px.mu.Unlock() // Unlock after updating shared variables.
		} else if reply.MaxObservedProposalID > maxObservedProposal { // Update the maximum observed proposal if needed.
			maxObservedProposal = reply.MaxObservedProposalID
		}
	})

	// Wait for all dispatched tasks to complete.
	px.waitGroup.Wait()

	// If we didn't get a majority of affirmative responses, return with failure.
	if affirmativeResponses < majorityCount(px.peers) {
		return false, nil, maxObservedProposal
	}

	// If a value was previously accepted, propose it for consensus.
	if highestPrevAcceptedProposal != -1 {
		return true, valueOfHighestPrevProposal, maxObservedProposal
	}

	// Propose the new value for consensus.
	return true, v, maxObservedProposal
}

// runAccept is the accept phase of the Paxos algorithm.
// It sends accept requests to all peers. If a majority of peers accept the value,
func (px *Paxos) runAccept(N int, seq int, vP interface{}) bool {
	//log.Printf("runAccept: Starting accept phase with ProposalID: %d, seq: %d, value: %v", N, seq, vP)

	// count tracks the number of acceptors that have accepted the proposal.
	var count int32

	// Concurrently dispatch accept requests to all peers.
	px.dispatchToPeers(func(i int, peer string) {
		// If a majority has been reached, there's no need to continue sending requests.
		if atomic.LoadInt32(&count) >= int32(majorityCount(px.peers)) {
			return
		}

		// Set up the accept request arguments.
		args := &AcceptArgs{
			BaseArgs:      BaseArgs{SequenceNumber: seq, ProposalID: N},
			AcceptedValue: vP,
		}

		var reply AcceptReply

		// Send the accept request to the peer.
		ok := px.remoteInvoke(peer, "Paxos.RPCAccept", args, &reply, i == px.me, px.handleAcceptorAccept)
		if ok && reply.Err == "" {
			// If accepted, update done value and increase the acceptance count.
			px.mu.Lock()
			px.adjustDoneValue(reply.AuxiliaryValue, i)
			atomic.AddInt32(&count, 1)
			px.mu.Unlock()
		}
	})

	// Return true if the acceptance count has achieved a majority.
	return atomic.LoadInt32(&count) >= int32(majorityCount(px.peers))
}

// runDecide checks if a proposed value has been decided upon by all peers.
func (px *Paxos) runDecide(N int, seq int, valueProposed interface{}) bool {

	// Construct RPC arguments.
	args := &DecidedArgs{
		BaseArgs:     BaseArgs{SequenceNumber: seq, ProposalID: N},
		DecidedValue: valueProposed,
	}

	successfulInvocations := 0

	handleSuccessfulInvocation := func() {
		px.mu.Lock()
		successfulInvocations++
		px.mu.Unlock()
	}

	/* error logging
	handleError := func(peer string, err string) {
		if err == "" {
			log.Printf("Error deciding for peer %s: Failed to invoke remote method.", peer)
		} else {
			log.Printf("Error deciding for peer %s: %s", peer, err)
		}
	}
	*/

	px.dispatchToPeers(func(i int, peer string) {
		var reply DecidedReply
		callSucceeded := px.remoteInvoke(peer, "Paxos.RPCDecide", args, &reply, i == px.me, px.handleAcceptorDecided)

		if callSucceeded && reply.Err == "" {
			handleSuccessfulInvocation()
		} /* Uncomment for error logging
		else {
			handleError(peer, reply.Err)
		}
		*/
	})

	// This waits for all goroutines in dispatchToPeers to complete
	px.waitGroup.Wait()

	// Ensure that the decision is informed to all the peers
	return successfulInvocations == len(px.peers)
}

// RPCPrepare processes the runPrepare request as part of the Paxos protocol.
// It checks if the proposal number is greater than any observed proposal number.
// If it is, it sends a positive response, otherwise it sends a refusal response.
func (px *Paxos) RPCPrepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	state := px.getStateOrCreate(args.SequenceNumber)

	// Refuse the proposal if its ID is less than or equal to the current state's proposal number.
	if args.ProposalID <= state.ProposalNumber {
		reply.MaxObservedProposalID = state.ProposalNumber
		reply.Err = "Refuse to accept"
		return nil
	}

	px.updateDoneValues(args.InitiatorID, args.LatestDoneSeq)
	reply.CurrentDone = px.getCurrentDoneValue()

	// Update current state's proposal number.
	px.updateCurrentState(state, args.ProposalID)
	px.setReplyFromState(state, reply)

	return nil
}

// RPCAccept processes the accept request as part of the Paxos protocol.
// If the proposal number is valid and higher than the current state's, it updates the state with the accepted proposal.
func (px *Paxos) RPCAccept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	state := px.getStateOrCreate(args.SequenceNumber)

	// Refuse to accept the proposal if its rank is not higher.
	if !px.isProposalRankHigher(args.ProposalID, state.ProposalNumber) {
		reply.Err = "Acceptor didn't accept."
		return nil
	}

	// Update state with the accepted proposal.
	px.updateStateWithAcceptedProposal(state, args)
	px.storeInstanceState(args.SequenceNumber, state)
	reply.AuxiliaryValue = px.loadDoneValue()

	return nil
}

// RPCDecide processes the decision request of the Paxos protocol.
// It marks the state as decided with the given value for the respective sequence number and proposal ID.
func (px *Paxos) RPCDecide(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	state := px.retrieveState(args.SequenceNumber)

	// If the state is already decided, no further action is needed.
	if px.isStateDecided(state) {
		//log (if necessary)
		return nil
	}

	// Mark the state as decided with the provided value.
	px.markStateAsDecided(state, args)
	px.storeInstanceState(args.SequenceNumber, state)

	return nil
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	go px.launchProposal(seq, v)
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation
func (px *Paxos) Done(seq int) {
	// Adjust the done value for the sequence
	px.adjustDoneValue(seq, px.me)

	// Clean up old instances
	threshold := px.Min()
	for index := 1; index < threshold; index++ {
		px.instances.Delete(index)
	}
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	var maxSeqNumber = -1

	keys := make([]int, 0)
	px.instances.Range(func(key, _ interface{}) bool {
		keys = append(keys, key.(int))
		return true
	})

	for _, key := range keys {
		if key > maxSeqNumber {
			maxSeqNumber = key
		}
	}

	return maxSeqNumber
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	lowestSeqNumber := math.MaxInt32

	values := make([]int, 0)
	px.doneValues.Range(func(_, value interface{}) bool {
		values = append(values, value.(int))
		return true
	})

	for _, value := range values {
		if value < lowestSeqNumber {
			lowestSeqNumber = value
		}
	}

	return lowestSeqNumber + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Check if the sequence is below the minimum threshold.
	if seq < px.Min() {
		return false, nil
	}

	// Attempt to load the current state of the instance.
	instanceState := px.loadInstanceState(seq)

	// If no state exists, return false with no associated value.
	if instanceState == nil {
		return false, nil
	}

	// Check if the state of the instance is decided and return the associated value.
	isDecidedState := instanceState.Status == "Decided"
	if isDecidedState {
		return true, instanceState.AcceptedValue
	}

	// Return false if the instance state isn't yet decided.
	return false, nil
}

// tell the peer to shut itself down.
// for testing.
// please do not change this function.
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = sync.Map{}
	px.doneValues = sync.Map{}
	px.waitGroup = sync.WaitGroup{}
	px.peerDone = make([]int, len(px.peers))
	px.initializeDoneValues()

	if rpcs != nil {
		// caller will create socket &c
		err := rpcs.Register(px)
		if err != nil {
			return nil
		}
	} else {
		rpcs = rpc.NewServer()
		err := rpcs.Register(px)
		if err != nil {
			return nil
		}

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		go px.garbageCleaning()

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
