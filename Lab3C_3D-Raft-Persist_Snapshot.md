# MIT 6.5840 Lab 3C/3D: Raft Persistence and Snapshots

> This note reviews my Lab 3C/3D implementation and the bugs that shaped its final design.
> It focuses on crash-safe Raft state, compacted-log indexing, snapshot transfer and recovery, ordered application, and the AppendEntries refactor motivated by excessive RPC counts.
> Selected snippets come from my current source at commit `6637e9f`; historical snippets are labeled explicitly. All displayed code uses two spaces per indentation level.

References:

- [Lab 3: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html)
- [In Search of an Understandable Consensus Algorithm (Extended Version)](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf), especially Figure 2, Figure 8, and Section 7
- My Git history and the saved `raft_3C_pass.log`, `raft_3D_pass.log`, `raft_old_pass.log`, and `raft_new_pass.log`

---

## 1. Goal and Scope

The three log-related stages form one recovery story:

```text
3B: replicate a log despite network failures
3C: preserve the log across process crashes
3D: discard old log entries without losing recovery ability
```

My Lab 3C/3D implementation must:

- persist the current term, vote, log, and compacted-log boundary;
- restore that state in `Make()` after a crash;
- preserve logical log indexes even when the physical slice no longer starts at index zero;
- accept snapshots created locally by the service through `Snapshot()`;
- send `InstallSnapshot` when a follower is behind the retained log;
- deliver snapshots and commands to the service in one monotonic order;
- and preserve the election and replication behavior from Labs 3A and 3B.

The Lab deliberately does not require membership changes, Byzantine-fault tolerance, snapshot chunking, or real-disk durability. `Persister` models stable storage in memory, and each snapshot is transferred in one RPC.

---

## 2. Final Result and Evidence Provenance

The saved results belong to different implementation stages. Keeping that provenance matters: a passing log is evidence for the revision that produced it, not automatically for every later edit.

### 2.1 Lab 3C stage

`raft_3C_pass.log` records 100 race-enabled rounds with maximum parallelism five:

```text
Passed rounds: 100
Failed rounds: 0
Average time sum: 145.083s
```

| Test | Average time | Runs |
|---|---:|---:|
| `TestFigure83C` | 39.238s | 100 |
| `TestFigure8Unreliable3C` | 40.323s | 100 |
| `TestPersist13C` | 5.355s | 100 |
| `TestPersist23C` | 18.569s | 100 |
| `TestPersist33C` | 2.750s | 100 |
| `TestReliableChurn3C` | 17.297s | 100 |
| `TestUnreliableAgree3C` | 4.253s | 100 |
| `TestUnreliableChurn3C` | 17.297s | 100 |

### 2.2 Lab 3D stage

`raft_3D_pass.log` records 100 race-enabled rounds with maximum parallelism five:

```text
Passed rounds: 100
Failed rounds: 0
Average time sum: 223.805s
```

| Test | Average time | Runs |
|---|---:|---:|
| `TestSnapshotAllCrash3D` | 16.215s | 100 |
| `TestSnapshotBasic3D` | 5.986s | 100 |
| `TestSnapshotInit3D` | 5.172s | 100 |
| `TestSnapshotInstall3D` | 44.476s | 100 |
| `TestSnapshotInstallCrash3D` | 39.906s | 100 |
| `TestSnapshotInstallUnCrash3D` | 50.764s | 100 |
| `TestSnapshotInstallUnreliable3D` | 61.287s | 100 |

### 2.3 Final replication-refactored implementation

Commit `6637e9f` introduced the per-follower replication workers and fixed the abnormal RPC-volume problem. `raft_new_pass.log` then ran the entire Lab 3 suite ten times:

```text
Passed rounds: 10
Failed rounds: 0
Maximum parallel jobs: 5
```

These repeated results are strong evidence against the races, crash schedules, partitions, and unreliable-network behaviors exercised by the tester. They are not a formal proof of Raft correctness or production durability.

---

## 3. Core Mental Model

Persistence and snapshots solve different loss problems:

```text
Persistence:
live Raft state -> serialized state that survives restart

Snapshot:
long committed log prefix -> state-machine image + boundary entry
```

A crash-recovery path is:

```text
persist
  -> crash
  -> Make
  -> readPersist + ReadSnapshot
  -> resume after index0
```

A lagging-follower recovery path is:

```text
nextIndex > index0
  -> AppendEntries

nextIndex <= index0
  -> InstallSnapshot
  -> AppendEntries from index0 + 1
```

The central snapshot idea is that removing storage is not the same as removing history. The state machine already contains the effects of commands through `index0`; Raft keeps the boundary term so later prefix checks still have an identity for that history.

---

## 4. State, Ownership, and Invariants

The final implementation adds snapshot and replication-worker state to the Lab 3B fields:

```go
log             []LogEntry
index0          int
snapshot        []byte
pendingSnapshot *raftapi.ApplyMsg
replicateCh     []chan struct{}
```

### Persistent and volatile state

| State                     | Persistent?     | Reason                                                                                                 |
| ------------------------- | --------------- | ------------------------------------------------------------------------------------------------------ |
| `currentTerm`             | Yes             | A restart must not move a peer into an older term.                                                     |
| `votedFor`                | Yes             | A restart must not permit two votes in one term.                                                       |
| `log`                     | Yes             | Accepted history must survive a crash.                                                                 |
| `index0`                  | Yes             | It defines the logical index represented by `log[0]`.                                                  |
| `snapshot`                | Yes, separately | It reconstructs service state through `index0`.                                                        |
| `commitIndex`             | No              | The current leader re-establishes commitment; restart initializes it at the durable snapshot boundary. |
| `lastApplied`             | No              | It tracks delivery in the current process; restart initializes it at the snapshot boundary.            |
| `nextIndex`, `matchIndex` | No              | They are leader-local replication progress and are rebuilt after election.                             |
| `pendingSnapshot`         | No              | It is an in-process delivery event.                                                                    |
| `replicateCh`             | No              | It is an in-process scheduling mechanism.                                                              |

The compacted log obeys these structural invariants:

```text
rf.log[0] represents logical index rf.index0
lastLogIndex = index0 + len(log) - 1
nextIndex <= index0 requires InstallSnapshot
ApplyMsg events must advance the service state monotonically
```

In steady state, after a pending snapshot has been delivered:

```text
index0 <= lastApplied <= commitIndex <= lastLogIndex
```

During `InstallSnapshot`, `index0` and `commitIndex` can advance before the applier delivers the pending snapshot, so `lastApplied < index0` is a deliberate short-lived state protected by `rf.mu`.

Ownership rules:

- shared protocol state is read and written under `rf.mu`;
- one `applier()` goroutine emits snapshot and command events;
- each follower has a dedicated replication notification channel and main catch-up worker;
- RPC arguments copy log entries and snapshot bytes before releasing `rf.mu`;
- network calls and `applyCh` sends occur without holding `rf.mu`.

---

## 5. Part 3C: Persistent State

Figure 2 of Raft identifies `currentTerm`, `votedFor`, and the log as persistent state. My compacted representation also needs `index0`.

### `currentTerm`

If a peer restarts in an older term, it may accept an obsolete leader or campaign with a term that the cluster has already passed. Persisting `currentTerm` prevents time from moving backward in the protocol.

### `votedFor`

Voting is an at-most-once decision per term. A peer that forgets its vote after a crash could grant a second vote in the same term, allowing two candidates to obtain different majorities across restart boundaries.

### `log`

Without a durable log, a restarted peer could accept a different command at an index that it had previously acknowledged. The majority and leader-completeness arguments depend on acknowledged history surviving crashes.

### `index0`

After compaction, the slice position no longer equals the Raft index. Persisting `log` without `index0` would preserve the bytes but destroy their logical meaning.

`commitIndex`, `lastApplied`, `nextIndex`, and `matchIndex` are volatile in the Raft specification. The first two are initialized to the durable snapshot boundary on restart; the last two are recreated whenever a peer becomes Leader.

---

## 6. Encoding and Restoring State

The current encoder writes four values in one fixed order and saves the snapshot through the `Persister`'s separate snapshot argument:

```go
func (rf *Raft) persist() {
  w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)
  e.Encode(rf.index0)
  e.Encode(rf.currentTerm)
  e.Encode(rf.votedFor)
  e.Encode(rf.log)
  raftstate := w.Bytes()
  rf.persister.Save(raftstate, rf.snapshot)
}
```

The decoder must consume exactly the same sequence:

```go
func (rf *Raft) readPersist(data []byte) {
  if len(data) < 1 {
    return
  }
  r := bytes.NewBuffer(data)
  d := labgob.NewDecoder(r)
  var currentTerm int
  var votedFor int
  var index int
  var log []LogEntry
  if d.Decode(&index) != nil ||
    d.Decode(&currentTerm) != nil ||
    d.Decode(&votedFor) != nil ||
    d.Decode(&log) != nil {
    slog.Error("PERSIST", "PEER", rf.me, "EVENT", "READ_PERSIST_FAILED")
  } else {
    rf.currentTerm = currentTerm
    rf.index0 = index
    rf.votedFor = votedFor
    rf.log = log
  }
}
```

The wire format is therefore:

```text
index0
currentTerm
votedFor
log
```

The snapshot bytes are not encoded again inside `raftstate`. `Save(raftstate, rf.snapshot)` stores them through the API's second field. This separation avoids duplicated bytes and matches the Lab's size accounting.

The important atomic relationship is conceptual: `index0`, the retained `log`, and `snapshot` must all describe the same boundary whenever `persist()` publishes them.

---

## 7. Persistence Mutation Points

The implementation persists every safety-relevant change.

Starting an election changes both term and vote:

```go
func (rf *Raft) beCandidate() int {
  electionTerm := rf.currentTerm + 1
  rf.currentTerm = electionTerm
  rf.votedFor = rf.me
  rf.state = Candidate
  rf.persist()
  return electionTerm
}
```

Adopting a newer term may clear the previous vote:

```go
func (rf *Raft) beFollower(term int) {
  if term > rf.currentTerm {
    rf.votedFor = -1
  }
  rf.state = Follower
  rf.currentTerm = term
  rf.persist()
}
```

Granting a vote sets `votedFor` and persists before the reply is returned:

```go
rf.votedFor = args.CandidateID
rf.persist()
*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
```

`Start()` persists the new Leader entry before returning it to the service:

```go
rf.log = append(rf.log, LogEntry{
  Command: command,
  Term:    rf.currentTerm,
})
rf.persist()
```

The follower persists term/log changes in `AppendEntries`. Both local `Snapshot()` and remote `InstallSnapshot()` persist the new boundary and snapshot bytes after updating them under the same lock.

> General rule: persist a safety-relevant state change before the operation is treated as complete or exposed through an RPC reply.

---

## 8. Restart Recovery

`Make()` restores Raft state first, then aligns volatile application progress with the durable snapshot boundary:

```go
rf.readPersist(persister.ReadRaftState())
rf.commitIndex = rf.index0
rf.lastApplied = rf.index0
rf.snapshot = persister.ReadSnapshot()
```

The service is responsible for restoring its state from the initial snapshot supplied by the Lab framework. Raft therefore starts command delivery after that snapshot rather than replaying compacted commands that no longer exist.

An earlier failure can be reconstructed with concrete indexes:

```text
restored index0 = 9
incorrect lastApplied = 0
commitIndex later becomes 16
applier requests Logs(1, 17)
physical start = 1 - 9 = -8
```

Initializing both `commitIndex` and `lastApplied` to `index0` restores the intended boundary:

```text
first newly applicable command index = index0 + 1
```

---

## 9. Logical and Physical Log Indexes

The final helpers centralize the mapping between protocol indexes and Go slice offsets:

```go
func (rf *Raft) lastLogIndex() int {
  return rf.index0 + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
  return rf.log[rf.lastLogIndex()-rf.index0].Term
}

func (rf *Raft) Logs(start, end int) []LogEntry {
  return rf.log[start-rf.index0 : end-rf.index0]
}

func (rf *Raft) Log(index int) LogEntry {
  return rf.log[index-rf.index0]
}
```

`Logs(start, end)` uses Go's half-open convention:

```text
[start, end)
```

For example, when `index0 = 9`:

| Slice offset | Logical index |
|---:|---:|
| 0 | 9, or `index0` |
| 1 | 10, or `index0+1` |
| 2 | 11, or `index0+2` |

This gives the two coordinate systems distinct meanings:

```text
logical Raft index = index0 + physical slice offset
physical slice offset = logical Raft index - index0
```

Every comparison, lookup, range, and splice after compaction must use logical indexes at the protocol boundary and convert them before indexing the slice. Centralizing that conversion made later failures much easier to diagnose.

---

## 10. Local Snapshot Creation

The service calls `Snapshot(index, snapshot)` only after its state machine contains all commands through `index`:

```go
func (rf *Raft) Snapshot(index int, snapshot []byte) {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  slog.Debug(
    "SNAPSHOT",
    "PEER", rf.me,
    "EVENT", "SNAPSHOT_CREATED",
    "INDEX", index,
    "RF_INDEX", rf.index0,
  )
  if index <= rf.index0 || index > rf.commitIndex {
    return
  }
  rf.snapshot = append([]byte{}, snapshot...)
  rf.log = append(
    []LogEntry{},
    rf.Logs(index, rf.lastLogIndex()+1)...,
  )
  rf.index0 = index
  rf.persist()
}
```

The guard has two meanings:

- `index <= index0`: this snapshot cannot compact anything new;
- `index > commitIndex`: the service must not snapshot uncommitted history.

The retained slice starts at `index`, not `index+1`. Its first entry is a dummy-like boundary entry whose term is still needed for `PrevLogTerm` checks and `LastIncludedTerm`. Its command is no longer applied because `lastApplied >= index0`.

The order of operations is important:

1. copy the snapshot bytes;
2. compute and copy the retained suffix using the old `index0`;
3. set the new `index0`;
4. persist the new Raft state and snapshot together.

The explicit copy creates a new backing array. Without it, the small retained suffix could keep the entire discarded prefix reachable and prevent garbage collection.

---

## 11. AppendEntries After Compaction

`AppendEntries` continues to speak in logical indexes. The follower first checks that the Leader's prefix position exists and has the expected term:

```go
if args.PrevLogIndex > rf.lastLogIndex() ||
  rf.Log(args.PrevLogIndex).Term != args.PrevLogTerm {
  *reply = AppendEntriesReply{
    Term:    rf.currentTerm,
    Success: false,
  }
  return
}
```

After finding the first absent or conflicting entry, the final splice retains the follower prefix and appends the Leader suffix:

```go
rf.log = append(
  rf.Logs(rf.index0, args.PrevLogIndex+i+1),
  args.Entries[i:]...,
)
```

Both bounds passed to `Logs` are logical. The helper converts them to physical offsets.

### Historical indexing bug

An earlier version used a logical index directly as a physical slice position. The observed state was:

```text
logical index 17 should exist
physical slice begins at a non-zero index0
direct slice indexing removes or misses index 17
the next Leader reports a last index near 27
```

The error was not merely an off-by-one. It mixed two coordinate systems. Once `index0 > 0`, `rf.log[17]` and `rf.Log(17)` refer to different physical elements. The final helpers make this distinction explicit.

---

## 12. Backtracking and the Snapshot Boundary

The Leader's retry path backs up by one local term at a time:

```go
func (rf *Raft) retryBack(
  server int,
  args *AppendEntriesArgs,
) {
  slog.Debug(
    "APPEND",
    "PEER", rf.me,
    "EVENT", "APPEND_ENTRIES_RETRY",
    "FOR", server,
  )
  // too-left-behind
  if rf.nextIndex[server] <= rf.index0+1 {
    rf.nextIndex[server] = rf.index0
    return
  }
  // back off by one term
  for index := args.PrevLogIndex - 1; index >= rf.index0; index-- {
    if index == rf.index0 {
      rf.nextIndex[server] = rf.index0 + 1
      break
    }
    if rf.Log(index).Term != rf.Log(index+1).Term {
      rf.nextIndex[server] = index + 1
      break
    }
  }
  // update args
  args.PrevLogIndex = rf.nextIndex[server] - 1
  args.PrevLogTerm = rf.Log(args.PrevLogIndex).Term
  args.Entries = append(
    []LogEntry{},
    rf.Logs(rf.nextIndex[server], rf.lastLogIndex()+1)...,
  )
}
```

The safety boundary is `index0`. The loop never calls `Log` below it. When `nextIndex <= index0+1`, the Leader records `nextIndex = index0`; the next call to `replicate(server)` selects `InstallSnapshot` before attempting `Log(nextIndex-1)`.

The extended Raft paper describes a faster optimization in which a rejected AppendEntries reply includes `XTerm`, `XIndex`, and `XLen`. My implementation instead uses the Leader's local term boundaries. It is simpler and passed the Lab tests, but a full conflict-metadata implementation can skip divergent history with fewer RPC round trips.

---

## 13. Sending and Receiving InstallSnapshot

### 13.1 Choosing the RPC

All catch-up rounds enter through `replicate(server)`. Its first log-position decision occurs while holding `rf.mu`:

```go
if rf.nextIndex[server] <= rf.index0 {
  args := InstallSnapshotArgs{
    Term:              rf.currentTerm,
    LeaderID:          rf.me,
    LastIncludedIndex: rf.index0,
    LastIncludedTerm:  rf.Log(rf.index0).Term,
    Data:              append([]byte{}, rf.snapshot...),
  }
  rf.mu.Unlock()
  return rf.sendInstallSnapshot(
    server,
    &args,
    &InstallSnapshotReply{},
  )
}
```

This branch must precede construction of normal AppendEntries. When `nextIndex <= index0`, `Log(nextIndex-1)` would address discarded history.

Otherwise the Leader sends the retained suffix:

```go
args := AppendEntriesArgs{
  Term:         rf.currentTerm,
  LeaderID:     rf.me,
  Entries: append(
    []LogEntry{},
    rf.Logs(rf.nextIndex[server], rf.lastLogIndex()+1)...,
  ),
  PrevLogIndex: rf.nextIndex[server] - 1,
  PrevLogTerm:  rf.Log(rf.nextIndex[server] - 1).Term,
  LeaderCommit: rf.commitIndex,
}
```

### 13.2 Sender processing

The snapshot sender treats transport failure as a reason for a later sequential retry:

```go
func (rf *Raft) sendInstallSnapshot(
  server int,
  args *InstallSnapshotArgs,
  reply *InstallSnapshotReply,
) bool {
  if !rf.peers[server].Call(
    "Raft.InstallSnapshot",
    args,
    reply,
  ) {
    slog.Debug(
      "SNAPSHOT",
      "PEER", rf.me,
      "EVENT", "INSTALL_SNAPSHOT_RPC_FAILURE",
      "TO", server,
    )
    return true
  }
  rf.mu.Lock()
  defer rf.mu.Unlock()
  if !rf.isCurrentLeader(args.Term, reply.Term) {
    return false
  }
  rf.matchIndex[server] = max(
    rf.matchIndex[server],
    args.LastIncludedIndex,
  )
  rf.nextIndex[server] = max(
    rf.nextIndex[server],
    args.LastIncludedIndex+1,
  )
  return rf.nextIndex[server] <= rf.lastLogIndex()
}
```

After the reply:

- a higher term makes this peer step down through `isCurrentLeader`;
- a reply for obsolete leadership cannot mutate progress;
- `max` prevents progress from moving backward;
- a `true` return asks the per-follower worker to replicate the suffix immediately.

### 13.3 Receiver processing

The receiver adopts a current/newer term before deciding whether the snapshot itself is stale:

```go
func (rf *Raft) InstallSnapshot(
  args *InstallSnapshotArgs,
  reply *InstallSnapshotReply,
) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  // old leader
  if args.Term < rf.currentTerm {
    *reply = InstallSnapshotReply{Term: rf.currentTerm}
    return
  }
  // new leader rpc
  select {
  case rf.heartBeatCh <- args.LeaderID:
  default:
  }
  rf.beFollower(args.Term)

  // old snapshot
  if args.LastIncludedIndex <= rf.commitIndex ||
    args.LastIncludedIndex <= rf.index0 {
    *reply = InstallSnapshotReply{Term: rf.currentTerm}
    return
  }
  if args.LastIncludedIndex <= rf.lastLogIndex() &&
    args.LastIncludedTerm ==
      rf.Log(args.LastIncludedIndex).Term {
    rf.log = append(
      []LogEntry{},
      rf.Logs(args.LastIncludedIndex, rf.lastLogIndex()+1)...,
    )
  } else {
    rf.log = []LogEntry{
      {Term: args.LastIncludedTerm},
    }
  }
  rf.index0 = args.LastIncludedIndex
  rf.snapshot = append([]byte{}, args.Data...)
  rf.commitIndex = args.LastIncludedIndex
  rf.pendingSnapshot = &raftapi.ApplyMsg{
    SnapshotValid: true,
    SnapshotTerm:  args.LastIncludedTerm,
    SnapshotIndex: args.LastIncludedIndex,
    Snapshot:      append([]byte{}, args.Data...),
  }
  rf.persist()
  *reply = InstallSnapshotReply{Term: rf.currentTerm}
  rf.cond.Broadcast()
}
```

The boundary is identified by the pair:

```text
(LastIncludedIndex, LastIncludedTerm)
```

Index equality alone is insufficient. If both index and term match an existing entry, Raft's Log Matching Property permits retaining the suffix after that entry. If the term differs or the entry does not exist, the receiver discards the entire old log and creates one boundary entry containing `LastIncludedTerm`.

`pendingSnapshot` carries the snapshot data, index, and term as one ordered event. Persistence happens before the applier is awakened.

---

## 14. Unified Command and Snapshot Applier

The final design has one producer for both kinds of `ApplyMsg`:

```go
func (rf *Raft) applier() {
  for {
    rf.mu.Lock()
    for rf.lastApplied >= rf.commitIndex &&
      rf.pendingSnapshot == nil {
      rf.cond.Wait()
    }
    if rf.pendingSnapshot != nil {
      msg := *rf.pendingSnapshot
      rf.lastApplied = msg.SnapshotIndex
      rf.pendingSnapshot = nil
      rf.mu.Unlock()
      rf.applyCh <- msg
      continue
    }
    start := rf.lastApplied + 1
    end := rf.commitIndex
    entries := append(
      []LogEntry{},
      rf.Logs(start, end+1)...,
    )
    rf.lastApplied = end
    rf.mu.Unlock()
    for i, entry := range entries {
      rf.applyCh <- raftapi.ApplyMsg{
        CommandValid: true,
        Command:      entry.Command,
        CommandIndex: start + i,
      }
    }
  }
}
```

Its ordering rule is:

```text
pending snapshot has priority
  -> deliver snapshot
  -> continue the loop
  -> deliver commands after SnapshotIndex
```

The condition-variable predicate waits only when there is neither a new command interval nor a pending snapshot. It must be checked in a loop because condition variables permit spurious wake-ups and another goroutine may change state before the waiter reacquires the lock.

The applier copies the committed entries and advances `lastApplied` under `rf.mu`, then sends outside the lock. This has three benefits:

1. `InstallSnapshot` cannot interleave a snapshot between two entries selected from the same committed interval;
2. a slow or blocked service cannot hold the Raft protocol mutex;
3. snapshot and command events share one total order.

The earlier design used separate apply goroutines and, at one stage, sent to `applyCh` while holding `rf.mu`. That allowed ordering races and lock/channel deadlocks. A single serialized applier fixed both classes of failure.

---

## 15. End-to-End Recovery Scenarios

### 15.1 Crash without compaction

```text
Leader appends entry
  -> persist term/vote/log
  -> follower acknowledges only after persisting
  -> process crashes
  -> Make calls readPersist
  -> current Leader re-establishes commitIndex
  -> applier delivers the durable suffix
```

Persistence protects the stored history; it does not by itself prove that an entry was committed.

### 15.2 Local compaction on all peers

```text
service receives commands through index S
  -> service creates snapshot containing their effects
  -> service calls Snapshot(S, bytes)
  -> Raft retains the boundary entry at S
  -> index0 becomes S
  -> Raft state and snapshot are saved together
```

Logical indexes do not change. Only their physical representation becomes smaller.

### 15.3 Disconnected follower behind `index0`

```text
follower disconnects
  -> Leader commits and compacts more entries
  -> follower rejoins with an old log
  -> AppendEntries rejection moves nextIndex backward
  -> nextIndex reaches index0
  -> Leader sends InstallSnapshot
  -> follower applies snapshot at index0
  -> Leader resumes AppendEntries at index0 + 1
```

Snapshot installation is therefore part of replication, not a separate maintenance feature.

### 15.4 Restart from an existing snapshot

```text
Persister contains index0, retained log, and snapshot
  -> service restores its initial state from snapshot
  -> Raft restores index0 and retained log
  -> commitIndex = lastApplied = index0
  -> first later command ApplyMsg has index index0 + 1
```

This is why restart initialization and service snapshot restoration must agree on the same boundary.

---

## 16. AppendEntries Replication Refactor

Commit `6637e9f` was not motivated by a correctness-test failure. The previous implementation passed the suite, but some failure-heavy cases used an abnormally large number of RPCs. The refactor changed replication ownership rather than the public Raft interface.

### 16.1 Symptom: abnormally high RPC counts

The clearest hotspot was `TestFigure8Unreliable3C`:

```text
before refactor, one run:       9656 RPCs
after refactor, ten-run mean:   2354.2 RPCs
reduction:                      approximately 75.6%
```

This is RPC amplification: one unit of useful replication progress causes many redundant network attempts.

High RPC volume matters even when tests pass:

- every request consumes CPU for encoding, dispatch, locking, and log checks;
- extra traffic increases congestion and the number of dropped or reordered messages in an unreliable network;
- more concurrent requests create more stale responses;
- redundant retry loops make `nextIndex` traces harder to interpret;
- later services built on Raft have less resource headroom.

### 16.2 Original parallel architecture

Before `6637e9f`, every agreement attempt called `sendAppendAll(term)`. It created a new RPC goroutine for each follower:

```go
// Before refactor: 6637e9f^
func (rf *Raft) sendAppendAll(term int) {
  for server := range rf.peers {
    if server == rf.me {
      continue
    }
    if rf.nextIndex[server] > rf.index0 {
      args := AppendEntriesArgs{
        Term:         term,
        LeaderID:     rf.me,
        Entries: append(
          []LogEntry{},
          rf.Logs(
            rf.nextIndex[server],
            rf.lastLogIndex()+1,
          )...,
        ),
        PrevLogIndex: rf.nextIndex[server] - 1,
        PrevLogTerm:  rf.Log(
          rf.nextIndex[server] - 1,
        ).Term,
        LeaderCommit: rf.commitIndex,
      }
      reply := AppendEntriesReply{}
      go rf.sendAppendEntries(server, &args, &reply)
    } else {
      args := InstallSnapshotArgs{
        Term:              term,
        LeaderID:          rf.me,
        LastIncludedIndex: rf.index0,
        LastIncludedTerm:  rf.Log(rf.index0).Term,
        Data:              append([]byte{}, rf.snapshot...),
      }
      reply := InstallSnapshotReply{}
      go rf.sendInstallSnapshot(server, &args, &reply)
    }
  }
}
```

Each temporary `sendAppendEntries` goroutine then owned its own retry loop:

```go
// Before refactor: 6637e9f^
func (rf *Raft) sendAppendEntries(
  server int,
  args *AppendEntriesArgs,
  reply *AppendEntriesReply,
) {
  for {
    if ok := rf.peers[server].Call(
      "Raft.AppendEntries",
      args,
      reply,
    ); !ok {
      return
    }
    rf.mu.Lock()
    if rf.state != Leader ||
      rf.currentTerm != args.Term {
      rf.mu.Unlock()
      return
    }
    if reply.Term > args.Term {
      defer rf.mu.Unlock()
      rf.beFollower(reply.Term)
      return
    }
    if reply.Success {
      defer rf.mu.Unlock()
      rf.nextIndex[server] = max(
        args.PrevLogIndex+len(args.Entries)+1,
        rf.nextIndex[server],
      )
      rf.matchIndex[server] = max(
        rf.matchIndex[server],
        rf.nextIndex[server]-1,
      )
      return
    }
    *reply = AppendEntriesReply{}
    if !rf.retryBack(server, args) {
      rf.mu.Unlock()
      return
    }
    rf.mu.Unlock()
  }
}
```

The triggering path was:

```text
heartbeat or Start
  -> sendAppendAll
  -> spawn one temporary sender per follower
  -> each sender may retry internally
```

A new heartbeat or `Start()` did not check whether a previous sender for the same follower was still waiting or backing up.

### 16.3 Why the parallel design amplified RPCs

Consider one slow follower:

```text
heartbeat H1 starts RPC A to follower 1
RPC A is delayed

new Start() starts RPC B to follower 1
heartbeat H2 starts RPC C to follower 1

A, B, and C all receive failures
all independently retry
all reason about nextIndex[1]
```

The mutex prevented a Go data race on `nextIndex[1]`, but it did not prevent redundant protocol work. Several goroutines could serialize their state updates while still maintaining separate retry streams based on different snapshots of the log and progress.

Under loss and delay, this becomes a feedback loop:

```text
delay or drop
  -> overlapping same-follower senders
  -> more RPC attempts
  -> more delayed and stale replies
  -> more independent retries
  -> still more traffic
```

The ownership problem was:

> No single component owned the ordered replication stream for one follower.

Cross-follower parallelism is desirable because one slow follower must not block another. Same-follower parallel catch-up is usually redundant because all attempts update the same `nextIndex[server]` and `matchIndex[server]`.

### 16.4 Refactor goals

The new design aimed to:

1. preserve parallelism across different followers;
2. serialize the main catch-up attempts for the same follower;
3. coalesce repeated replication notifications;
4. retry immediately while a follower remains behind;
5. stop when leadership or term changes;
6. select AppendEntries or InstallSnapshot from one entry point;
7. prevent old responses from moving progress backward;
8. reduce RPC amplification without changing the public API.

### 16.5 New per-follower replication structure

`Raft` now contains one notification channel per peer:

```go
replicateCh []chan struct{} // Signals for each replicator
```

`Make()` creates capacity-one channels and one long-lived worker for every other peer:

```go
rf.replicateCh = make([]chan struct{}, len(peers))
for i := range peers {
  if i != me {
    rf.replicateCh[i] = make(chan struct{}, 1)
  }
}

// ...

for i := range peers {
  if i != me {
    go rf.replicator(i)
  }
}
```

The worker is intentionally small:

```go
func (rf *Raft) replicator(server int) {
  for range rf.replicateCh[server] {
    for rf.replicate(server) {
    }
  }
}
```

The outer loop sleeps until replication may be useful. The inner loop owns a sequential catch-up session and continues while `replicate(server)` reports that another round is needed.

A follower therefore has:

```text
one notification slot
  + one long-lived worker
  + one sequential main catch-up loop
```

### 16.6 Notification coalescing

`Start()` eventually calls `notifyAllReplicators()` through `startAgreement`:

```go
func (rf *Raft) notifyAllReplicators() {
  for server := 0; server < len(rf.peers); server++ {
    if server == rf.me {
      continue
    }
    select {
    case rf.replicateCh[server] <- struct{}{}:
    default:
    }
  }
}
```

Capacity one gives the channel set-like semantics:

```text
zero pending signals -> enqueue one
one pending signal   -> discard duplicate notification
```

The message does not mean "replicate exactly one entry." It means:

```text
replication state may have changed; inspect current Raft state
```

If ten commands arrive while a follower worker is busy, the channel holds at most one additional wake-up. The worker reads the current `nextIndex` and current last-log index when it runs, so one notification can cover many entries.

### 16.7 Unified one-round replication

`replicate(server)` is the state-driven entry point for one network round:

```go
func (rf *Raft) replicate(server int) bool {
  rf.mu.Lock()
  if rf.state != Leader {
    rf.mu.Unlock()
    return false
  }

  if rf.nextIndex[server] <= rf.index0 {
    args := InstallSnapshotArgs{
      Term:              rf.currentTerm,
      LeaderID:          rf.me,
      LastIncludedIndex: rf.index0,
      LastIncludedTerm:  rf.Log(rf.index0).Term,
      Data:              append([]byte{}, rf.snapshot...),
    }
    rf.mu.Unlock()
    return rf.sendInstallSnapshot(
      server,
      &args,
      &InstallSnapshotReply{},
    )
  }

  args := AppendEntriesArgs{
    Term:         rf.currentTerm,
    LeaderID:     rf.me,
    Entries: append(
      []LogEntry{},
      rf.Logs(
        rf.nextIndex[server],
        rf.lastLogIndex()+1,
      )...,
    ),
    PrevLogIndex: rf.nextIndex[server] - 1,
    PrevLogTerm:  rf.Log(
      rf.nextIndex[server] - 1,
    ).Term,
    LeaderCommit: rf.commitIndex,
  }
  rf.mu.Unlock()
  return rf.sendAppendEntries(
    server,
    &args,
    &AppendEntriesReply{},
  )
}
```

One round:

1. locks Raft state;
2. verifies that this peer is still Leader;
3. reads the current `nextIndex`;
4. chooses InstallSnapshot or AppendEntries;
5. copies mutable log/snapshot data into RPC-owned arguments;
6. unlocks before the network call;
7. processes the reply and returns whether another round is needed.

The sender no longer contains an unbounded private retry loop:

```go
func (rf *Raft) sendAppendEntries(
  server int,
  args *AppendEntriesArgs,
  reply *AppendEntriesReply,
) bool {
  if !rf.peers[server].Call(
    "Raft.AppendEntries",
    args,
    reply,
  ) {
    return true
  }
  rf.mu.Lock()
  defer rf.mu.Unlock()
  if !rf.isCurrentLeader(args.Term, reply.Term) {
    return false
  }
  if !reply.Success {
    rf.retryBack(server, args)
    return true
  }
  rf.nextIndex[server] = max(
    args.PrevLogIndex+len(args.Entries)+1,
    rf.nextIndex[server],
  )
  rf.matchIndex[server] = max(
    rf.matchIndex[server],
    rf.nextIndex[server]-1,
  )
  return rf.nextIndex[server] <= rf.lastLogIndex()
}
```

The boolean transfers retry ownership back to the dedicated worker.

### 16.8 Old and new code comparison

| Concern | Original implementation | Refactored implementation |
|---|---|---|
| Work trigger | Each agreement/heartbeat path could create RPC goroutines. | `Start()` uses a buffered notification. |
| Same-follower concurrency | Multiple private retry loops could overlap. | One dedicated worker owns the main catch-up loop. |
| Duplicate work | Every trigger created work. | Capacity-one signals coalesce. |
| Retry ownership | Each temporary sender goroutine. | The follower's long-lived `replicator`. |
| RPC choice | Append/snapshot choice lived in the send-all path. | `replicate` centralizes the choice. |
| Stale-response surface | Multiple same-follower requests could race on progress. | The main catch-up stream is ordered. |
| Goroutine lifetime | Many short-lived sender/retry goroutines. | One long-lived worker per follower. |
| Cross-follower parallelism | Yes. | Yes; each follower has its own worker. |

The core code difference is:

```go
// Before refactor
go rf.sendAppendEntries(server, &args, &reply)

// After refactor
case rf.replicateCh[server] <- struct{}{}:
```

and:

```go
// Before refactor
func sendAppendEntries(...) {
  for {
    Call(...)
    retryBack(...)
  }
}

// After refactor
func replicator(server int) {
  for range replicateCh[server] {
    for replicate(server) {
    }
  }
}
```

The new loop is not just moved code. It gives the retry stream a stable owner.

### 16.9 Correctness and reasoning benefits

Serialization makes these per-follower properties easier to maintain:

```text
nextIndex[server] describes one ordered main stream of attempts
matchIndex[server] never moves backward
a retry is interpreted after the previous attempt
snapshot installation precedes suffix replication
```

Response validation is centralized:

```go
func (rf *Raft) isCurrentLeader(
  argsTerm,
  replyTerm int,
) bool {
  if replyTerm > rf.currentTerm {
    rf.beFollower(replyTerm)
    return false
  }
  return rf.state == Leader &&
    rf.currentTerm == argsTerm
}
```

This handles two different invalidation cases:

- a higher reply term forces step-down;
- even without a higher reply term, an RPC from an obsolete Leader term must not mutate current progress.

The update still uses `max` after serialization. That is intentional defensive monotonicity: delayed transport responses or replication invoked through another path must not move `nextIndex` or `matchIndex` backward.

### 16.10 RPC performance comparison

The baseline and result have unequal sample sizes:

```text
raft_old_pass.log: one complete-suite run before refactor
raft_new_pass.log: mean of ten complete-suite runs after refactor
```

For each row:

```text
reduction = (old - new) / old * 100%
```

| Test | Old RPCs (1 run) | New mean RPCs (10 runs) | Reduction |
|---|---:|---:|---:|
| `TestBackup3B` | 1955 | 1397.4 | 28.5% |
| `TestUnreliableAgree3C` | 1088 | 656.0 | 39.7% |
| `TestFigure8Unreliable3C` | 9656 | 2354.2 | 75.6% |
| `TestCount3B` | 54 | 45.1 | 16.5% |
| `TestSnapshotBasic3D` | 502 | 375.1 | 25.3% |
| `TestSnapshotInstall3D` | 1295 | 1070.9 | 17.3% |
| `TestSnapshotInstallUnreliable3D` | 1537 | 1264.3 | 17.7% |
| `TestSnapshotInstallCrash3D` | 1136 | 767.3 | 32.5% |
| `TestSnapshotInstallUnCrash3D` | 1236 | 775.1 | 37.3% |

The strongest measured result is:

```text
TestFigure8Unreliable3C:
9656 -> 2354.2 RPCs
approximately 75.6% fewer RPCs
```

This workload combines divergence, repeated Leader changes, and unreliable communication: the conditions most likely to expose overlapping retry streams. The magnitude of the reduction directly supports the refactor's stated motivation.

The table is intentionally selective. Some tests changed little, and some RPC counts increased. For example, `TestReliableChurn3C` increased from 1427 in the single old run to a ten-run mean of 1828.2. This is another reason to describe the result as fixing the observed amplification hotspots rather than promising a universal reduction for every schedule.

### 16.11 Wall-clock performance interpretation

The complete-suite time sums were:

```text
raft_old_pass.log: 462.130s, one run
raft_new_pass.log: 491.209s average, ten runs
```

The logs do not establish a universal wall-clock speedup. The new average is higher, and several snapshot-install tests took longer despite fewer RPCs. This does not contradict the RPC result:

- unreliable tests have high scheduling and network-delay variance;
- the old baseline has one sample, while the new result averages ten;
- serializing redundant requests can reduce load without shortening the critical path;
- snapshot and election timing may dominate elapsed time;
- fewer requests measure efficiency, not automatically latency.

> The refactor successfully fixed the abnormal RPC-volume problem, especially under unreliable and divergent-log workloads, while preserving correctness. The available logs do not establish a universal wall-clock speedup.

### 16.12 Boundary of the current serialization

The current code does not mathematically guarantee one in-flight RPC per follower from every trigger.

`Start()`-driven catch-up uses the capacity-one channels:

```go
func (rf *Raft) startAgreement(index, term int) {
  rf.mu.Lock()
  rf.notifyAllReplicators()
  rf.mu.Unlock()
  rf.waitMajorityAgreement(index, term)
}
```

However, `heartBeat()` still directly starts a replication goroutine:

```go
for server := range rf.peers {
  if server != rf.me {
    go rf.replicate(server)
  }
}
```

A heartbeat round can therefore overlap the dedicated worker for the same follower. The accurate description of the implemented improvement is:

> Serial ownership of the main retry/catch-up path with coalesced Start notifications, substantially reducing same-follower RPC amplification.

Why the `heartBeat()` acts like this is because the heart beat should be surely forwarded to followers. Otherwise the follower may miss some heart beat and start a new election.

---

## 17. Safety, Liveness, and Failure Model

### Safety

The most relevant safety properties are:

- **Term monotonicity across restart:** persisted `currentTerm` prevents a peer from returning to an older term.
- **At most one vote per term:** persisted `votedFor` survives crashes.
- **Durable accepted history:** persisted log entries cannot disappear merely because a process restarts.
- **Snapshot boundary identity:** `(index0, Log(index0).Term)` represents the compacted prefix.
- **Monotonic service state:** one applier orders snapshot and command delivery.
- **Stale-Leader rejection:** RPC receivers reject older terms, and senders revalidate replies before changing progress.
- **Monotonic replication evidence:** `matchIndex` and successful `nextIndex` updates use `max`.

A snapshot does not weaken Log Matching. It replaces a known committed prefix while preserving its last index and term.

### Liveness

Progress requires a communicating majority and eventually stable timing. Under those assumptions:

- elections can select a Leader;
- the Leader retries unavailable or dropped RPCs;
- a follower with retained overlapping history catches up through AppendEntries;
- a follower behind `index0` catches up through InstallSnapshot;
- `cond.Broadcast()` wakes the applier after commitment or snapshot installation;
- replication channels wake long-lived per-follower workers without accumulating an unbounded queue.

Serialization improves resource liveness by avoiding redundant retry storms, but it does not remove the network and majority assumptions of Raft.

### Failure model

The Lab exercises:

- process crash and restart;
- all-server crash and restart;
- request or response loss;
- delayed and reordered RPC completion;
- network partition and later rejoin;
- stale Leaders and higher terms;
- divergent follower logs;
- followers that lag behind the compacted prefix.

It does not model Byzantine peers, corrupted stable storage, snapshot authentication, partial snapshot chunks, or production disk/fsync behavior.

---

## 18. Bug Retrospective

Each bug below is recorded as symptom, root cause, and final correction.

### Bug 1: Persist/decode order mismatch

**Symptom**

```text
PERSIST PEER=n EVENT=READ_PERSIST_FAILED
```

or restored fields contained values of the wrong logical type.

**Root cause**

The decoder did not consume values in exactly the order used by the encoder. Serialization is positional; field names do not travel with the bytes.

**Final fix**

Use one documented order in both functions:

```text
index0 -> currentTerm -> votedFor -> log
```

**Transferable lesson**

Treat a persistence layout as a protocol. Changing either side requires changing and testing the other side.

### Bug 2: Holding `rf.mu` while sending to `applyCh`

**Symptom**

Raft stopped making progress while the service/tester was slow or waiting on another Raft action.

**Root cause**

A channel send can block. Holding the protocol mutex during the send prevented RPC handlers and state transitions from acquiring it, creating a lock/channel dependency cycle.

**Final fix**

Copy the message or log interval and advance the protected cursor under `rf.mu`, then unlock before sending.

**Transferable lesson**

Never treat a channel send as a cheap assignment. Its blocking behavior is part of the lock graph.

### Bug 3: Parallel apply goroutines reordered state-machine events

**Symptom**

Different peers applied different values or a snapshot raced with command delivery even though the committed log was correct.

**Root cause**

Separate goroutines independently sent committed entries. Go scheduling does not preserve the creation order of goroutines.

**Final fix**

Create exactly one long-lived `applier()` and route both snapshot and command events through it.

**Transferable lesson**

A state machine needs one serialization point. Correct commitment is not enough if delivery can reorder commands.

### Bug 4: Reversed applier wait predicate

**Symptom**

The applier either slept when committed work existed or ran when no range could be safely selected.

**Root cause**

The condition confused "caught up" with "work available."

**Final fix**

Wait while both conditions are true:

```go
for rf.lastApplied >= rf.commitIndex &&
  rf.pendingSnapshot == nil {
  rf.cond.Wait()
}
```

**Transferable lesson**

Write condition-variable predicates as the exact state in which sleeping is safe, and re-check them in a loop.

### Bug 5: Logical index used as physical slice index

**Symptom**

A retained entry such as logical index 17 disappeared, and later Leaders started from unexpectedly large indexes.

**Root cause**

After `index0 > 0`, protocol index `i` is stored at physical offset `i-index0`, not at `rf.log[i]`.

**Final fix**

Centralize all access in `Log(i)` and `Logs(start, end)`, and keep their arguments logical.

**Transferable lesson**

When a data structure has virtual coordinates, encode the conversion once and forbid ad hoc indexing.

### Bug 6: `retryBack` crossed below `index0`

**Symptom**

A failure-heavy 3D test panicked with an index-out-of-bounds access while backing up a lagging follower.

**Root cause**

The retry loop treated compacted entries as if they still existed and attempted `Log(index)` below the retained boundary.

**Final fix**

Stop at `index0`. Set `nextIndex = index0` when the follower is too far behind so the next round chooses InstallSnapshot before any log lookup.

**Transferable lesson**

Compaction changes the valid domain of every algorithm that walks history, not only the code that deletes it.

### Bug 7: Snapshot bytes duplicated inside `raftstate`

**Symptom**

Persisted Raft-state size stayed large or violated the snapshot size expectations even after log compaction.

**Root cause**

The snapshot was encoded into the Raft-state buffer and also passed as the second argument to `Persister.Save`.

**Final fix**

Encode only `index0`, `currentTerm`, `votedFor`, and `log`; save `rf.snapshot` only through the separate snapshot field.

**Transferable lesson**

Storage APIs often separate metadata from bulk data for both semantics and accounting. Respect that boundary.

### Bug 8: Restart restored `index0` but not application progress

**Symptom**

After restart, the applier requested a range beginning below the compacted boundary and panicked with a negative physical slice offset.

**Root cause**

`readPersist` restored `index0`, but `lastApplied` and `commitIndex` remained at zero.

**Final fix**

In `Make()`:

```go
rf.commitIndex = rf.index0
rf.lastApplied = rf.index0
```

**Transferable lesson**

Restoring durable representation often requires reinitializing volatile cursors to a derived boundary.

### Bug 9: Incomplete pending-snapshot representation

**Symptom**

The service could receive snapshot bytes without enough metadata to order or validate the state transition.

**Root cause**

A boolean such as `pendingSnapshot` described only that work existed, not which snapshot had to be applied.

**Final fix**

Store the complete `raftapi.ApplyMsg`:

```go
rf.pendingSnapshot = &raftapi.ApplyMsg{
  SnapshotValid: true,
  SnapshotTerm:  args.LastIncludedTerm,
  SnapshotIndex: args.LastIncludedIndex,
  Snapshot:      append([]byte{}, args.Data...),
}
```

**Transferable lesson**

A queued state transition should be self-contained. Re-reading mutable global fields later can combine data from different versions.

### Bug 10: Same-follower parallel replication caused RPC storms

**Symptom**

The suite passed, but `TestFigure8Unreliable3C` used 9656 RPCs in the saved pre-refactor run.

**Root cause**

Every trigger could create another sender for a follower while previous senders were delayed or retrying. The mutex protected variables but did not establish one owner for the retry stream.

**Final fix**

Add capacity-one `replicateCh` notifications and one long-lived `replicator` per follower. The detailed mechanism and measurements are in Section 16.

**Transferable lesson**

Data-race freedom is weaker than concurrency correctness. Redundant owners can produce a protocol-level retry storm even when every shared write is locked.

### Bug 11: Higher-term step-down retained `votedFor`

**Symptom**

After observing a newer term, a peer could still believe its vote from an older term applied, causing incorrect vote rejection or delayed elections.

**Root cause**

Role transition updated `currentTerm` but did not reset the term-scoped vote.

**Final fix**

Clear the vote only when the term increases:

```go
if term > rf.currentTerm {
  rf.votedFor = -1
}
```

Then persist the new term and vote together.

**Transferable lesson**

When a version/epoch changes, audit every field whose meaning is scoped to that epoch.

---

## 19. Testing Strategy

The evidence-producing commands were:

```bash
./test.sh 3C raft1 100 5
./test.sh 3D raft1 100 5
./test.sh "" raft1 10 5
```

The script builds the race-enabled daemon and invokes race-enabled tests. Each layer answers a different question:

| Evidence | What it helps detect | What it does not prove |
|---|---|---|
| `-race` | Unsynchronized memory accesses in executed schedules | Protocol correctness or absence of deadlocks |
| One focused test | Fast feedback on a known path | Rare schedules |
| 100 repeated 3C/3D rounds | Intermittent crash, snapshot, and timing failures | All possible executions |
| Full-suite rounds | Regression across 3A-3D | Production behavior |
| Saved failure logs | A concrete distributed event sequence | Root cause without analysis |
| RPC counts | Traffic amplification and retry efficiency | End-to-end latency |
| Operation counts | Amount of tester-visible work | Internal correctness by itself |
| Wall-clock time | Observed elapsed time in that environment | Stable performance from unequal samples |

For the refactor comparison, every percentage in Section 16 was recalculated from the two saved files using:

```text
(old RPCs - new mean RPCs) / old RPCs * 100%
```

The comparison always states the sample sizes: one old run versus a ten-run new mean. No fresh long-running test was executed while writing this note; the verification here audited the saved outputs and current source.

---

## 20. Known Limitations and Design Tradeoffs

- Snapshots are sent in one RPC, as required by the Lab. Production systems may need chunking, checksums, resumable transfer, and rate limiting.
- Backtracking uses local term boundaries instead of follower-provided `XTerm`, `XIndex`, and `XLen`; it may need more round trips.
- `Persister` models durable storage but does not model fsync ordering, torn writes, disk corruption, or storage latency.
- Membership changes and joint consensus are not implemented.
- Byzantine peers and malicious snapshot data are outside the failure model.

---

## 21. Five-Minute Explanation

Lab 3C makes Raft's safety-relevant history survive crashes. I encode `index0`, `currentTerm`, `votedFor`, and `log` in one fixed order, while the snapshot bytes use the separate snapshot field in `Persister`. Every mutation to term, vote, log, or snapshot boundary is persisted before the action is considered complete.

Lab 3D compacts a committed prefix without changing its logical indexes. `log[0]` represents logical `index0`, and helper functions translate logical indexes into slice offsets. A local snapshot retains the entry at the boundary so its term remains available. If a follower's `nextIndex` moves to or before `index0`, the Leader sends `InstallSnapshot`; a matching boundary index and term allows the follower to retain its suffix, while a mismatch discards it.

One applier serializes snapshot and command delivery. It prioritizes a pending snapshot, advances `lastApplied` to the snapshot index, then resumes commands at the next logical index. It copies work under `rf.mu` and sends outside the mutex.

The later replication refactor addressed a performance pathology. The old design could start multiple retry loops for the same follower, so network delays caused RPC amplification. The new design gives each follower a capacity-one notification channel and a long-lived worker that owns the main sequential catch-up loop. It preserves parallelism across followers while coalescing duplicate Start notifications. In the strongest saved case, `TestFigure8Unreliable3C` dropped from 9656 RPCs in one old run to a ten-run mean of 2354.2, about 75.6% fewer. The logs support improved RPC efficiency, not a universal wall-clock speedup, and the direct heartbeat path remains a serialization limitation.

---

## 22. Self-Check

1. Why must `votedFor` survive a crash even though it is reset in a newer term?
2. Why is persisting `log` insufficient if `index0` is lost?
3. Why does the compacted representation retain the boundary entry's term?
4. Exactly when must the Leader choose InstallSnapshot instead of AppendEntries?
5. Why do snapshots and commands share one applier?
6. Why does the applier copy entries under `rf.mu` but send them after unlocking?
7. How can same-follower RPC concurrency create stale work even without a Go data race?
8. Why should different followers still replicate concurrently?
9. How does a capacity-one channel coalesce ten `Start()` notifications into one unit of pending work?
10. Why does a 75.6% RPC reduction not imply a 75.6% latency reduction?
11. Which current heartbeat path prevents a strict "one in-flight RPC per follower" claim?
12. What do the 100-round 3C/3D results and ten full-suite rounds demonstrate, and what do they not prove?

---

## 23. Final Takeaways

- Persistence prevents Raft's term, vote, and accepted history from moving backward after restart.
- Snapshotting removes physical storage while preserving logical log indexes.
- `(LastIncludedIndex, LastIncludedTerm)` identifies the compacted history boundary.
- Restoring `commitIndex` and `lastApplied` to `index0` aligns volatile progress with durable state.
- One ordered applier prevents snapshot/command races and avoids blocking Raft while holding its mutex.
- One logical replication owner per follower makes retries and progress easier to reason about.
- Capacity-one notifications coalesce redundant work without losing the fact that replication is needed.
- The refactor reduced the largest observed RPC hotspot by approximately 75.6%.
- The evidence supports lower RPC amplification, not a universal latency improvement.
- Repeated race-enabled testing is necessary evidence for a concurrent implementation, but it is not a formal proof.
