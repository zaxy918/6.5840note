# MIT 6.5840 Lab 3A: Raft Leader Election

> This note reviews my Lab 3A implementation and the concurrency bugs I encountered.  
> It focuses on the mental model, invariants, failure evidence, and the actual source code that implements leader election and heartbeats.

Reference: [In Search of an Understandable Consensus Algorithm (Extended Version)](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)

---

## 1. Goal and Scope

Lab 3A implements the leader-election part of Raft:

```text
start as follower
    ↓ election timeout
become candidate and request votes
    ↓ majority
become leader and send heartbeats
    ↓ newer term
step down to follower
```

The tests cover:

- initial leader election
- reelection after disconnecting a leader
- and repeated elections under changing network connectivity

This part does not yet implement command replication, commit and apply, persistence, or snapshots.

### Final Result

My final repeated run reported:

```text
Passed rounds: 1000
Failed rounds: 0

Average time by test case:
  TestInitialElection3A                      4.406s  (1000 runs)
  TestManyElections3A                        8.462s  (1000 runs)
  TestReElection3A                           5.909s  (1000 runs)
Average time sum of all test cases: 18.777s
```

This is strong evidence for the tested 3A scenarios. It is not proof that the implementation already satisfies every rule needed by the complete Raft protocol.

---

## 2. Core Mental Model

![[pictures/raft-algorithm.png]]

Every Raft peer is in one of three states:

```go
type State int

const (
	Follower State = iota
	Candidate
	Leader
)
```

### Follower

A follower waits for a heartbeat or a vote request. If no heartbeat arrives before its election timeout, it starts an election.

### Candidate

A candidate:

1. advances to a new term
2. votes for itself
3. sends `RequestVote` RPCs in parallel
4. and becomes leader after receiving a majority

### Leader

A leader periodically sends empty `AppendEntries` RPCs. In Lab 3A these are heartbeats rather than log replication.

```text
heartbeat = "I am still the leader for this term"
```

The state transitions are:

```text
Follower -- timeout --> Candidate
Candidate -- majority --> Leader
Candidate -- newer term/leader --> Follower
Leader -- newer term --> Follower
```

---

## 3. Term and Majority

### Term

A term is the logical generation number of an election:

```text
Term 1 -> Term 2 -> Term 3 -> ...
```

Every election and RPC carries a term. A message from an older term is stale. A peer that observes a newer term must stop acting as a leader or candidate.

### Majority

My candidate uses:

```go
if cntYes > len(rf.peers)/2 {
```

| Cluster size | Majority |
|---:|---:|
| 3 | 2 |
| 5 | 3 |
| 7 | 4 |

Any two majority sets overlap. If each peer casts at most one vote in a term, two candidates cannot both obtain a valid majority in that term.

---

## 4. Safety, Liveness, and Failure Model

### Safety

The central Lab 3A safety property is:

> At most one leader may be elected in a given term.

The implementation protects this property with:

- monotonically increasing terms
- majority voting
- stale-message rejection
- and final state validation before becoming leader

### Liveness

If a majority of peers can communicate, the cluster should eventually elect a leader.

Liveness depends on:

- randomized election timeouts
- parallel vote RPCs
- finite RPC returns
- and an available majority

If no partition contains a majority, Raft cannot elect a leader. This is expected.

### Failure Model in 3A

The important cases are:

- leader disconnection
- delayed RPC replies
- overlapping elections
- stale leaders reconnecting
- and concurrent goroutine scheduling

Later labs add dropped messages, crashes, restarts, persistence, and log inconsistency.

---

## 5. State and Ownership

My Lab 3A state is:

```go
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state

	me          int   // this peer's index into peers[]
	currentTerm int   // The term server has seen for the last time
	votedFor    int   // Candidate id that received vote
	leaderId    int   // The current leader's id
	state       State // The state of the server: Follower, Candidate or Leader
	log         []LogEntry
	heartBeatCh chan int
}
```

Ownership rules:

- `currentTerm`, `votedFor`, `leaderId`, `state`, and `log` are protected by `rf.mu`.
- Each RPC goroutine owns its own request and reply objects.
- `heartBeatCh` belongs to the Raft peer and lives across election cycles.
- One local election is identified by its captured `electionTerm`.

### Meaning of `me`

The tester passes `me` into `Make()`. It is this peer's index in the common `peers` array.

For three peers:

```text
me ∈ {0, 1, 2}
```

It is used as the local peer ID, candidate ID, leader ID, and the index skipped during RPC broadcast.

---

## 6. Initialization

Every peer begins as a follower and starts a timer goroutine:

```go
rf := &Raft{}
rf.peers = peers
rf.persister = persister
rf.me = me
rf.heartBeatCh = make(chan int)
rf.state = Follower
// initialize from state persisted before a crash
rf.readPersist(persister.ReadRaftState())

// start ticker goroutine to start elections
go rf.ticker()

return rf
```

`Make()` must return quickly, so election work runs in a goroutine.

---

## 7. Election Timer

My randomized timeout is 300–450ms:

```go
// The election timeout is 300 ~ 450ms
func electionTimeout() time.Duration {
	return time.Duration((300 + rand.Int63()%150)) * time.Millisecond
}
```

Randomization reduces split votes. Identical timeouts would make many followers become candidates simultaneously.

The actual ticker is:

```go
func (rf *Raft) ticker() {
	timer := time.NewTimer(electionTimeout())
	defer timer.Stop()
	for {
	Loop:
		for {
			select {
			case leaderId := <-rf.heartBeatCh:
				slog.Debug("received heartbeat from leader; staying follower",
					"peer_id", rf.me,
					"leader_id", leaderId,
				)
				break Loop
			case <-timer.C:
				slog.Debug("election timeout elapsed",
					"peer_id", rf.me,
				)
				if rf.election() == Leader {
					slog.Debug("election won; starting heartbeat loop",
						"peer_id", rf.me,
					)
					return
				}
				slog.Debug("election lost or timed out; staying follower",
					"peer_id", rf.me,
				)
				break Loop
			}
		}
		timer.Reset(electionTimeout())
	}
}
```

### Why `break Loop` Is Necessary

A plain `break` inside `select` exits only the `select`. The labeled break exits the inner loop and reaches:

```go
timer.Reset(electionTimeout())
```

Receiving a heartbeat therefore begins a fresh randomized election period.

---

## 8. Starting an Election

The transition to candidate happens while holding `rf.mu`:

```go
electionTerm := rf.currentTerm + 1
rf.currentTerm = electionTerm
rf.votedFor = rf.me
rf.state = Candidate
```

The candidate sends concurrent vote requests:

```go
votes := make(chan bool, len(rf.peers))
for server := range rf.peers {
	if server != rf.me {
		args := RequestVoteArgs{
			Term:         electionTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm()}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(server, electionTerm, votes, &args, &reply)
	}
}
rf.mu.Unlock()
cntYes := 1
cntNo := 0
```

Each RPC receives its own `args` and `reply`. The candidate also counts its self-vote.

The vote channel is buffered:

```go
votes := make(chan bool, len(rf.peers))
```

If the election returns after reaching a majority, late RPC goroutines can still publish a result and exit. The unreachable channel can later be reclaimed by GC.

---

## 9. RequestVote

The RPC types are:

```go
type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateId  int
	LastLogIndex int // Index of the candidate's last log entry
	LastLogTerm  int // Term of the candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm
	VoteGranted bool // true if the cnadidate received vote
}
```

My current eligibility check is:

```go
func (rf *Raft) shouldGrantVote(args *RequestVoteArgs) bool {
	if args.Term <= rf.currentTerm {
		return false
	}
	if args.LastLogTerm < func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Term
		}
		return 0
	}() {
		return false
	}
	if args.LastLogIndex < len(rf.log) {
		return false
	}
	return true
}
```

The state change in the handler is:

```go
if rf.shouldGrantVote(args) {
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	if rf.state != Follower {
		rf.state = Follower
	}
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
} else {
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
}
```

> [!caution] Lab 3A scope
> This passed my 3A tests, but later parts should align voting exactly with Raft Figure 2: update the term whenever a higher term is observed, track one vote per term, handle valid duplicate requests, and compare log freshness by last term and then last index.

---

## 10. Collecting Votes Safely

The RPC result is accepted only when it belongs to the election term:

```go
if reply.Term == electionTerm {
	slog.Debug("vote reply is still valid for this term",
		"peer_id", rf.me,
		"target_peer", server,
		"vote_granted", reply.VoteGranted,
		"election_term", electionTerm,
	)
	votes <- reply.VoteGranted
}
```

The candidate uses a bounded timer:

```go
voteTimeout := time.NewTimer(500 * time.Millisecond)
defer voteTimeout.Stop()
```

Before becoming leader, it checks the state and term again:

```go
if rf.state == Candidate && rf.currentTerm == electionTerm {
	slog.Debug("election won",
		"peer_id", rf.me,
		"term", rf.currentTerm,
		"leader_id", rf.currentTerm,
		"votes_yes", cntYes,
		"votes_no", cntNo,
	)
	rf.state = Leader
	rf.leaderId = rf.me
	// Start heartbeat goroutine
	go rf.heartBeat()
	return Leader
}
```

This second check is essential because another RPC may have advanced the term while `election()` was blocked in `select`.

The old result is valid only when:

```text
state == Candidate && currentTerm == electionTerm
```

---

## 11. Heartbeats

The leader sends empty `AppendEntries` requests:

```go
args := AppendEntriesArgs{
	Term:     rf.currentTerm,
	LeaderId: rf.me,
	Entries:  nil}
reply := AppendEntriesReply{}
go rf.sendAppendEntries(server, shutDown, &args, &reply)
```

The heartbeat interval is 150ms, shorter than the election timeout.

The optimized lock boundary in my heartbeat loop is:

```go
rf.mu.Lock()
if rf.state == Leader && rf.currentTerm == leaderTerm {
	slog.Debug("broadcasting heartbeat to peers",
		"peer_id", rf.me,
		"term", rf.currentTerm,
		"leader_id", rf.me,
	)
	rf.mu.Unlock()
	shutDown := make(chan int, len(rf.peers))
```

The mutex is released before network calls and timer waits.

> [!note] Future refinement
> Later implementations should copy all RPC fields into local variables while holding `rf.mu`, then send an immutable snapshot after unlocking.

---

## 12. Receiving Heartbeats

The follower rejects an old leader:

```go
} else {
	slog.Debug("AppendEntries from an old leader",
		"peer_id", rf.me,
		"leader_id", args.LeaderId,
		"leader_term", args.Term,
		"local_term", rf.currentTerm,
	)
	*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false}
}
```

For a current or newer leader, it updates:

```go
rf.currentTerm = args.Term
rf.leaderId = args.LeaderId
rf.state = Follower
```

It notifies the ticker without blocking:

```go
select {
case rf.heartBeatCh <- args.LeaderId:
default:
}
```

This is a signal rather than an event queue. Repeated pending heartbeat notifications add no useful information.

### Why `heartBeatCh` Is Not Closed

The channel belongs to the Raft peer, not one call to `ticker()` or `AppendEntries()`. It remains open for the peer's lifetime. When the peer and all related goroutines become unreachable, Go can reclaim it.

---

## 13. Old Leader Step-Down

A disconnected leader may continue believing it is leader while a majority partition elects a newer leader.

When the old leader receives a newer term:

```go
if reply.Term > rf.currentTerm {
	slog.Debug("AppendEntries reply came from a newer term; stepping down",
		"peer_id", rf.me,
		"target_peer", server,
		"reply_term", reply.Term,
	)
	rf.state = Follower
	rf.currentTerm = reply.Term
	shutDown <- reply.Term
}
```

The heartbeat loop exits:

```go
case <-shutDown:
	slog.Debug("heartbeat loop received a newer-term notification; switching back to ticker",
		"peer_id", rf.me,
	)
	go rf.ticker()
	return
```

The newer term invalidates the old leader's authority.

---

## 14. Concurrency Rules Learned

### Rule 1: Keep the Critical Section Short

The intended pattern is:

```text
lock -> read/update shared state -> unlock
perform RPC/timer wait
lock -> validate result -> update state -> unlock
```

RPC calls, timer waits, and potentially blocking channel operations should not happen while holding `rf.mu`.

### Rule 2: Recheck After Blocking

State can change while waiting in:

- `select`,
- an RPC,
- a timer,
- or a channel operation.

A condition checked before blocking cannot authorize a later transition without revalidation.

### Rule 3: Do Not Share RPC Replies

Each concurrent RPC needs its own reply object. Otherwise the RPC framework writes the same memory from multiple goroutines.

### Rule 4: Closing Depends on Ownership

A receiver does not automatically own the right to close a channel. The long-lived `heartBeatCh` should not be closed after one election or ticker cycle.

---

## 15. Bug Retrospective

### Bug 1: Shared Vote Reply

**Symptom:** the race detector reported concurrent access during vote RPCs.

**Root cause:** multiple goroutines shared one `RequestVoteReply`.

**Current fix:**

```go
args := RequestVoteArgs{
	Term:         electionTerm,
	CandidateId:  rf.me,
	LastLogIndex: rf.lastLogIndex(),
	LastLogTerm:  rf.lastLogTerm()}
reply := RequestVoteReply{}
go rf.sendRequestVote(server, electionTerm, votes, &args, &reply)
```

Each RPC owns its arguments and reply.

---

### Bug 2: Heartbeat Handler Blocked

**Symptom:** a peer logged the start of `AppendEntries()` and then stopped responding.

**Root cause:** it sent to `heartBeatCh` while holding `rf.mu`, but no receiver was ready.

**Current fix:**

```go
select {
case rf.heartBeatCh <- args.LeaderId:
default:
}
```

The RPC handler cannot block on the notification.

---

### Bug 3: Old Heartbeat Goroutine Self-Deadlocked

**Symptom:** after stepping down, a peer disappeared from later logs and stopped answering RPCs.

**Root cause:** one loop branch kept `rf.mu` locked and tried to lock it again on the next iteration.

**Current fix:**

```go
} else {
	slog.Debug("heartbeat loop exiting; no longer leader or term changed",
		"peer_id", rf.me,
		"term", rf.currentTerm,
	)
	rf.mu.Unlock()
	go rf.ticker()
	return
}
```

Every lock path now has a matching unlock.

---

### Bug 4: Old Election Won in a New Term

**Symptom:**

```text
Fatal: term 15 has 2 (>1) leaders
```

**Root cause:** a peer stepped down after observing a newer term, then received a delayed vote from an older election and promoted itself without rechecking state.

**Current fix:**

```go
if rf.state == Candidate && rf.currentTerm == electionTerm {
	rf.state = Leader
	rf.leaderId = rf.me
	// Start heartbeat goroutine
	go rf.heartBeat()
	return Leader
}
```

Only the currently active election may promote the peer.

---

### Bug 5: `break` Exited Only `select`

**Root cause:** a plain `break` inside `select` did not reach the timer reset logic.

**Current fix:**

```go
break Loop
```

---

### Bug 6: Heartbeat Wait Held `rf.mu`

**Symptom:** RPC handlers, replies, and `GetState()` waited unnecessarily for the heartbeat interval.

**Root cause:** the leader held the global mutex while waiting for the 150ms timer.

**Current fix:**

```go
if rf.state == Leader && rf.currentTerm == leaderTerm {
	slog.Debug("broadcasting heartbeat to peers",
		"peer_id", rf.me,
		"term", rf.currentTerm,
		"leader_id", rf.me,
	)
	rf.mu.Unlock()
```

This was the main performance optimization after correctness stabilized.

---

## 16. Testing Strategy

The basic race check is:

```bash
go test -race -run 3A
```

My repeated test runner is:

```bash
./test.sh 3A raft1 1000 <max-parallel>
```

The script:

- builds the daemon with `-race`,
- runs repeated rounds with bounded parallelism,
- preserves failed logs,
- removes passed logs,
- and reports timing statistics.

### Why Repetition Matters

A concurrency bug may pass once because its scheduling order did not occur. Repetition increases exposure to:

- delayed replies,
- overlapping elections,
- simultaneous timeouts,
- old-leader reconnection,
- and rare lock/channel interleavings.

### Limits of the Evidence

The 1000-round result does not verify:

- unreliable RPC behavior,
- persistence across crashes,
- log replication safety,
- snapshot recovery,
- or production-scale performance.

Those require later labs and additional invariants.

---

## 17. Five-Minute Explanation

My implementation starts every peer as a follower with a randomized election timer. When the timer expires, the peer becomes a candidate, advances its term, votes for itself, and sends `RequestVote` RPCs concurrently. It becomes leader only after receiving a majority and rechecking that it is still a candidate in the same term.

The leader sends empty `AppendEntries` RPCs every 150ms. Followers use these heartbeats to reset their election timers. Every RPC carries a term, so stale leaders and candidates can be rejected. A leader that observes a newer term steps down and restarts follower behavior.

The most important concurrency lessons were to give each RPC its own reply object, avoid blocking channel sends while holding the Raft mutex, release the mutex before RPC and timer waits, and revalidate state after every blocking operation.

---

## 18. Self-Check

1. Why does majority intersection prevent two valid leaders in one term?
2. Why must a candidate recheck `state` and `currentTerm` after receiving the final vote?
3. Why does an election timeout mean “no heartbeat was observed” rather than “the old leader definitely crashed”?
4. Why can the buffered vote channel become unreachable without being closed?
5. Under what partition can a seven-peer cluster continue electing a leader?
6. Which parts of the current `RequestVote` rule must be generalized for later labs?

---

## 19. Final Takeaways

- Term identifies the generation of an election.
- Majority intersection is the foundation of election safety.
- Randomized timeouts support liveness.
- Heartbeats suppress unnecessary elections.
- A newer term invalidates old leadership.
- Shared Raft state belongs behind `rf.mu`.
- RPC calls, timer waits, and blocking channel operations do not belong in the critical section.
- Delayed replies must be checked against the election that created them.
- Concurrent RPCs must not share mutable reply objects.
- Passing 1000 race-enabled rounds is meaningful evidence for Lab 3A, but complete Raft correctness still depends on later labs.
