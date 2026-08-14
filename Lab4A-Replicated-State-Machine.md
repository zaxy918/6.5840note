# MIT 6.5840 Lab 4A: Replicated State Machine

> This note explains my Lab 4A replicated state machine layer between a service and Raft. The implementation identifies each submitted operation, waits for the exact command chosen at Raft's returned log index, executes committed commands through one `Reader` goroutine, and returns `ErrWrongLeader` when leadership changes. A pre-fix 1,000-round run exposed two rare `TestRestartSubmit4A` failures caused by an abandoned unbuffered result channel blocking the apply path. The final buffered-channel implementation is recorded in commit `aa2c528`, and the final evidence committed by `c097b70` passed 1,000 of 1,000 complete 4A rounds with `-race`.

| Item | Value |
|---|---|
| Course | MIT 6.5840 Spring 2026 |
| Lab | Lab 4A: replicated state machine (`kvraft1/rsm`) |
| Source revision | Implementation `aa2c528`; evidence bundle `c097b70` |
| Test status | 1,000/1,000 complete 4A rounds passed; 7,000 test-case executions |
| Evidence | `rsm.go`, `reader.go`, `rsm_test.go`, `rsm_4A_pass.log`, two retained failure logs |

---

## 1. Preparation

### 1.1 Goal

Lab 4A builds a service-independent replicated state machine layer. A service calls `Submit(request)`. The RSM sends the request through Raft, executes committed requests in Raft order, and returns the result produced by the service's `DoOp` method.

The layer must handle concurrent submissions, leader replacement, partitions, shutdown, and restart. Safety requires every replica to execute the same committed commands in the same order. A `Submit` call must return success only for its own operation. Liveness requires requests to finish when a stable Leader can communicate with a majority; an isolated old Leader may wait until the partition heals.

This matches the official Lab 4A interface: `Submit` calls `raft.Start`, while one reader consumes `applyCh`, calls `DoOp`, and routes the result back to the waiting submission.

### 1.2 Starting Point

| Area | Starting behavior | Required Lab 4A behavior |
|---|---|---|
| Service request | Direct call with no replication | Wrap the request and submit it through Raft |
| Commit delivery | Raw `ApplyMsg` from Raft | Execute `DoOp` exactly in apply order |
| Result delivery | No relationship between `Start` and `applyCh` | Match a committed log index to the waiting `Submit` |
| Leader change | A started operation may never commit | Return `ErrWrongLeader` after term or role changes |
| Log overwrite | The same index may contain another Leader's command | Verify the committed operation identity |
| Shutdown/restart | Waiters and volatile state disappear | Stop obsolete waiters and rebuild service state by replay |

### 1.3 Mental Model

<!-- schematic: author-generated explanation -->

```text
service RPC goroutine
  -> RSM.Submit(request)
  -> Raft.Start(Op) returns (index, term)
  -> wait on result channel associated with index

Raft applier
  -> applyCh[index]
  -> RSM.Reader
  -> StateMachine.DoOp(request)
  -> result channel[index]
  -> Submit returns to the service
```

> [!IMPORTANT]
> A Raft log index identifies a position, not permanently one request. If leadership changes, another command may be chosen at the same index. Success therefore requires both the expected index and the expected operation identity.

---

## 2. Operation Identity and State Ownership

### 2.1 Identifying an Operation

Concurrent callers need an identity that survives encoding through Raft. I use the submitting peer and a peer-local atomic sequence number.

<!-- evidence: kind=source revision=c097b700 repo=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840 path=src/kvraft1/rsm/rsm.go lines=16-20 sha256=c2d06f830ff11c0d0bfbd6b88cf1e152eb2b4191f5bf003297f977c351da5cf2 -->

```go
type Op struct {
	Me      int
	ID      uint64
	Request any
}
```

`ID` distinguishes concurrent operations originating from one peer. `Me` prevents peer 0's operation 1 from being confused with peer 1's operation 1. All fields are exported because the operation is encoded into the Raft log.

### 2.2 Shared State

The RSM owns the bridge between log positions and waiting service goroutines.

<!-- evidence: kind=source revision=c097b700 repo=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840 path=src/kvraft1/rsm/rsm.go lines=34-47 sha256=c2d06f830ff11c0d0bfbd6b88cf1e152eb2b4191f5bf003297f977c351da5cf2 -->

```go
type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	opID     atomic.Uint64
	opResChs map[int]chan applyResult
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
```

| State | Owner | Lifetime | Safety role |
|---|---|---|---|
| `opID` | Submitting RSM peer | One RSM process | Creates peer-local operation IDs |
| `opResChs[index]` | `Submit` registers; `Reader` consumes | Until the index is applied | Routes an execution result to a waiter |
| `applyCh` | Raft produces; one `Reader` consumes | One RSM process | Preserves the committed apply order |
| service state | `Reader` through `DoOp` | Rebuilt after restart | Holds the deterministic application result |

`opResChs` is protected by `rsm.mu`. The atomic counter does not require the mutex. A single reader owns state-machine execution, so concurrent client requests cannot execute outside Raft order.

### 2.3 Initialization

`MakeRSM` initializes the result map and starts the long-lived apply reader. The reader starts before Raft, but initially blocks on `applyCh`, so `MakeRSM` still returns quickly.

<!-- evidence: kind=source revision=c097b700 repo=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840 path=src/kvraft1/rsm/rsm.go lines=61-74 sha256=c2d06f830ff11c0d0bfbd6b88cf1e152eb2b4191f5bf003297f977c351da5cf2 -->

```go
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		opResChs:     make(map[int]chan applyResult),
	}
	go rsm.Reader()
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	return rsm
}
```

---

## 3. Submitting and Waiting for the Chosen Operation

### 3.1 `Submit`

`Submit` first rejects a known non-Leader. A Leader wraps the service request, calls `Start`, records the returned index, and installs a result channel before releasing `rsm.mu`.

<!-- evidence: kind=source revision=c097b700 repo=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840 path=src/kvraft1/rsm/rsm.go lines=83-126 sha256=c2d06f830ff11c0d0bfbd6b88cf1e152eb2b4191f5bf003297f977c351da5cf2 -->

```go
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	term, isLeader := rsm.rf.GetState()
	// not leader
	if !isLeader {
		slog.Debug(
			"SUBMIT",
			"PEER", rsm.me,
			"EVENT", "NOT_LEADER",
		)
		return rpc.ErrWrongLeader, nil
	}
	// construct op
	op := Op{
		Me:      rsm.me,
		ID:      rsm.opID.Add(1),
		Request: req,
	}
	// start raft agreement
	slog.Debug(
		"SUBMIT",
		"PEER", rsm.me,
		"EVENT", "START",
		"OP", op.ID,
	)
	// start the operation
	rsm.mu.Lock()
	index, nterm, isLeader := rsm.rf.Start(op)
	if nterm != term || !isLeader {
		slog.Debug(
			"SUBMIT",
			"PEER", rsm.me,
			"EVENT", "LEADER_CHANGE",
			"OP", op.ID,
		)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	// create a channel for the result
	resCh := make(chan applyResult, 1)
	rsm.opResChs[index] = resCh
	rsm.mu.Unlock()
	// wait for the result
	return rsm.waitApply(term, op, resCh)
}
```

Holding `rsm.mu` across `Start` and map registration closes a fast-commit race. Even if Raft immediately emits the command, `Reader` must acquire the same mutex before looking up `opResChs`, so the waiter is visible first.

The channel capacity is one. Result delivery must not require the original waiting goroutine to still be alive. This small detail became the decisive fix for the restart failure in Section 5.

### 3.2 `waitApply`

A started command has three relevant outcomes:

1. the exact operation is committed and executed, so return `OK` and its result;
2. another operation is chosen at the same index, so return `ErrWrongLeader`;
3. the peer loses its term or Leader role before receiving a result, so return `ErrWrongLeader`.

<!-- evidence: kind=source revision=c097b700 repo=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840 path=src/kvraft1/rsm/rsm.go lines=128-164 sha256=c2d06f830ff11c0d0bfbd6b88cf1e152eb2b4191f5bf003297f977c351da5cf2 -->

```go
func (rsm *RSM) waitApply(term int, op Op, resCh chan applyResult) (rpc.Err, any) {
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case res := <-resCh:
			if res.op.ID != op.ID || res.op.Me != op.Me {
				slog.Debug(
					"SUBMIT",
					"PEER", rsm.me,
					"EVENT", "WRONG_OP",
					"OP", op.ID,
					"RES", res.op.ID,
				)
				return rpc.ErrWrongLeader, nil
			}
			slog.Debug(
				"SUBMIT",
				"PEER", rsm.me,
				"EVENT", "SUBMIT_SUCCESS",
				"OP", op.ID,
			)
			return rpc.OK, res.value
		case <-timer.C:
			timer.Reset(500 * time.Millisecond)
			if nterm, isLeader := rsm.rf.GetState(); nterm != term || !isLeader {
				slog.Debug(
					"SUBMIT",
					"PEER", rsm.me,
					"EVENT", "LEADER_CHANGE",
					"OP", op.ID,
				)
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
```

The timer is a leadership check, not an operation timeout. An isolated Leader may still report the same term and role, so the call can wait until communication resumes. This behavior is allowed by the lab because a client isolated with that Leader cannot reach the majority either.

### 3.3 Concrete Leader-Change Execution

<!-- schematic: author-generated explanation -->

```text
Leader A: Start(opA) -> index i, term T
  -> A loses leadership before i commits
  -> Leader B chooses opB at index i
  -> A's Reader receives opB at index i
  -> waitApply compares (Me, ID)
  -> opB != opA
  -> Submit(opA) returns ErrWrongLeader
```

> [!WARNING]
> Checking only `CommandIndex` would let an old Leader report success for another Leader's command. Checking only the current term would miss an overwrite already delivered through `applyCh`. Both position and operation identity matter.

---

## 4. Applying Commands and Returning Results

### 4.1 One Ordered Reader

Raft already determines the committed command order. The RSM preserves it by using one `Reader` goroutine to call `DoOp` sequentially. Every replica executes the command even when that replica has no local waiter. Only the Leader that originated a still-waiting submission normally finds an entry in `opResChs`.

<!-- evidence: kind=source revision=c097b700 repo=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840 path=src/kvraft1/rsm/reader.go lines=5-38 sha256=c9dd6191eafdb62157c4b4219c0e9af2c8cb3f1c315ef59f7ea77076919da54e -->

```go
type applyResult struct {
	op    Op
	value any
}

func (rsm *RSM) Reader() {
	for msg := range rsm.applyCh {
		// invalid commands
		if !msg.CommandValid {
			slog.Debug(
				"READ",
				"PEER", rsm.me,
				"EVENT", "INVALID_COMMAND",
				"OP", msg.Command.(Op).ID,
			)
			continue
		}
		slog.Debug(
			"READ",
			"PEER", rsm.me,
			"EVENT", "DO_OP",
			"OP", msg.Command.(Op).ID,
		)
		res := rsm.sm.DoOp(msg.Command.(Op).Request)
		rsm.mu.Lock()
		if resCh, ok := rsm.opResChs[msg.CommandIndex]; ok {
			delete(rsm.opResChs, msg.CommandIndex)
			rsm.mu.Unlock()
			resCh <- applyResult{op: msg.Command.(Op), value: res}
			continue
		}
		rsm.mu.Unlock()
	}
}
```

The important ordering is:

```text
receive committed ApplyMsg
  -> execute DoOp
  -> remove the waiter for that index
  -> publish the exact Op and result
```

`DoOp` runs before the result is returned, so `Submit` success means the local state machine has executed the operation. The result send occurs without `rsm.mu`, avoiding a lock dependency between the reader and a waiting request.

### 4.2 Safety and Liveness Invariants

| Invariant | Why it holds in the current 4A design |
|---|---|
| Commands execute in committed order | One reader consumes the ordered `applyCh` |
| Followers also update service state | `DoOp` is called before checking for a local waiter |
| A waiter receives at most one apply result | The map entry is deleted before the send |
| An overwritten operation does not report success | `waitApply` compares both `Me` and `ID` |
| A departed waiter cannot stop the reader | The result channel has capacity one |

---

## 5. Restart Failure: An Abandoned Waiter Blocked Apply

### 5.1 Symptom

An early 1,000-round stress run passed 998 complete rounds and failed twice. Every failure occurred in `TestRestartSubmit4A`; the other six tests passed all 1,000 executions.

<!-- evidence: kind=log path=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840%5Csrc%5Ctest_log%5Clab4%5C20260814%5C4A_1000_10_default_20260814182957.log lines=2012-2033 sha256=f5ba1427c7ac3046cb94660abb7156c0595471bad51839649a95c1e70c84d1e1 -->

```text
Test case statistics
Suite  Test case                                    Avg time   Peers   Avg RPCs    Avg Ops   Pass   Fail   Runs
------ ------------------------------------------ ---------- ------- ---------- ---------- ------ ------ ------
4A     TestBasic4A                                    2.338s       3       44.0       10.0   1000      0   1000
4A     TestConcurrent4A                               0.968s       3       27.9       50.0   1000      0   1000
4A     TestLeaderFailure4A                            1.808s       3       31.3        2.0   1000      0   1000
4A     TestLeaderPartition4A                          2.703s       3      137.2        2.0   1000      0   1000
4A     TestRestartReplay4A                           18.738s       3      424.1      101.0   1000      0   1000
4A     TestShutdown4A                                10.661s       3        0.4        0.0   1000      0   1000
4A     TestRestartSubmit4A                           29.808s       3      436.8      102.0    998      2   1000

Invocation summary
  Invocation timestamp: 20260814182957
  Summary log path: /home/zaxy/studyspace/projects/6.5840/src/test_log/lab4/20260814/4A_1000_10_default_20260814182957.log
  Debug log root/date: /home/zaxy/studyspace/projects/6.5840/src/debug_log/lab4/<job-start-date>
  Total jobs: 1000
  Passed jobs: 998
  Failed jobs: 2
  4A rounds: passed=998 failed=2; average time sum=67.024s
  Retained failure logs:
    /home/zaxy/studyspace/projects/6.5840/src/debug_log/lab4/20260814/4A_round287_20260814190150.log
    /home/zaxy/studyspace/projects/6.5840/src/debug_log/lab4/20260814/4A_round477_20260814192329.log
```

The tester expected all three state machines to reach counter value 102, but only two did. Meanwhile, the lagging peer continued receiving heartbeats and `AppendEntries`, so Raft communication was alive while service application had stopped.

### 5.2 Representative Event Sequence

Round 287 shows a short-lived Leader on peer 0. It started operation 1 at index 114 in term 3, stepped down, and returned `LEADER_CHANGE`. Peer 1 then became Leader and started the retried operation at index 120 in term 4.

<!-- evidence: kind=log path=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840%5Csrc%5Cdebug_log%5Clab4%5C20260814%5C4A_round287_20260814190150.log lines=5650-5671 sha256=8d9718756eb8fbcb084dc12b5b51e9a0cbacad2af495678fc1331aed76b4edc6 -->

```text
[875ms] SUBMIT PEER=0 EVENT=START OP=1
[467ms] APPEND PEER=2 EVENT=HEARTBEAT_RECEIVED FROM=0
[467ms] APPEND PEER=2 EVENT=SHOULD_RETRY
[876ms] AGREEMENT PEER=0 EVENT=START_AGREEMENT INDEX=114 TERM=3
[671ms] VOTE PEER=1 EVENT=REQUEST_VOTE TO=0 TERM=4
[671ms] VOTE PEER=1 EVENT=REQUEST_VOTE TO=2 TERM=4
[876ms] APPEND PEER=0 EVENT=APPEND_ENTRIES_RETRY FOR=2
[469ms] STEP_DOWN PEER=2 EVENT=STEP_DOWN TERM=4
[469ms] VOTE PEER=2 EVENT=GRANT_VOTE TO=1
[469ms] APPEND PEER=2 EVENT=HEARTBEAT_RECEIVED FROM=1
[878ms] STEP_DOWN PEER=0 EVENT=STEP_DOWN TERM=4
[879ms] STEP_DOWN PEER=0 EVENT=STEP_DOWN TERM=4
[675ms] VOTE PEER=1 EVENT=BE_LEADER TERM=4
[472ms] APPEND PEER=2 EVENT=HEARTBEAT_RECEIVED FROM=1
[881ms] APPEND PEER=0 EVENT=SHOULD_RETRY
[677ms] APPEND PEER=1 EVENT=APPEND_ENTRIES_RETRY FOR=0
[474ms] APPEND PEER=2 EVENT=SHOULD_RETRY
[678ms] APPEND PEER=1 EVENT=APPEND_ENTRIES_RETRY FOR=2
[927ms] SUBMIT PEER=0 EVENT=LEADER_CHANGE OP=1
[775ms] SUBMIT PEER=1 EVENT=START OP=1
[777ms] AGREEMENT PEER=1 EVENT=START_AGREEMENT INDEX=120 TERM=4
[575ms] APPEND PEER=2 EVENT=HEARTBEAT_RECEIVED FROM=1
```

Peer 0 later replayed the winning history but stopped producing `READ` events immediately after operation 10. Peer 2 continued through the final operation 1.

<!-- evidence: kind=log path=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840%5Csrc%5Cdebug_log%5Clab4%5C20260814%5C4A_round287_20260814190150.log lines=6020-6038 sha256=8d9718756eb8fbcb084dc12b5b51e9a0cbacad2af495678fc1331aed76b4edc6 -->

```text
[777ms] READ PEER=2 EVENT=DO_OP OP=45
[1186ms] READ PEER=0 EVENT=DO_OP OP=44
[777ms] READ PEER=2 EVENT=DO_OP OP=44
[1186ms] READ PEER=0 EVENT=DO_OP OP=7
[1186ms] READ PEER=0 EVENT=DO_OP OP=8
[1186ms] READ PEER=0 EVENT=DO_OP OP=9
[1186ms] READ PEER=0 EVENT=DO_OP OP=12
[1186ms] READ PEER=0 EVENT=DO_OP OP=10
[777ms] READ PEER=2 EVENT=DO_OP OP=7
[777ms] READ PEER=2 EVENT=DO_OP OP=8
[777ms] READ PEER=2 EVENT=DO_OP OP=9
[777ms] READ PEER=2 EVENT=DO_OP OP=12
[778ms] READ PEER=2 EVENT=DO_OP OP=10
[778ms] READ PEER=2 EVENT=DO_OP OP=13
[778ms] READ PEER=2 EVENT=DO_OP OP=11
[778ms] READ PEER=2 EVENT=DO_OP OP=14
[778ms] READ PEER=2 EVENT=DO_OP OP=15
[778ms] READ PEER=2 EVENT=DO_OP OP=16
[778ms] READ PEER=2 EVENT=DO_OP OP=1
```

The test finally reported that only two replicas reached 102, even though the third peer was still processing Raft heartbeats.

<!-- evidence: kind=log path=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840%5Csrc%5Cdebug_log%5Clab4%5C20260814%5C4A_round287_20260814190150.log lines=6848-6856 sha256=8d9718756eb8fbcb084dc12b5b51e9a0cbacad2af495678fc1331aed76b4edc6 -->

```text
[31757ms] APPEND PEER=0 EVENT=HEARTBEAT_RECEIVED FROM=1
[31349ms] APPEND PEER=2 EVENT=APPENDING
[31759ms] APPEND PEER=0 EVENT=APPENDING
Fatal: checkCounter: only 2 srvs have 102 instead of 3
        /home/zaxy/studyspace/projects/6.5840/src/kvraft1/rsm/test.go:177
        /home/zaxy/studyspace/projects/6.5840/src/kvraft1/rsm/rsm_test.go:314
info: wrote visualization to /tmp/porcupine-920230013.html
--- FAIL: TestRestartSubmit4A (60.60s)
FAIL
```

### 5.3 Root Cause and Fix

The pre-fix worktree used an unbuffered result channel. When peer 0 lost leadership, `waitApply` returned, but `opResChs[114]` remained. The new Leader's longer history replaced the old command at index 114. When peer 0 later applied the winning command at that index, `Reader` found the abandoned map entry and sent to its result channel. No goroutine was receiving, so the single reader blocked forever.

This root-cause reconstruction is supported by both retained raw logs and the pre-fix code inspected during debugging. The exact pre-fix source was not committed, so this note does not present it as a Git quote. The current committed line is the actual fix: `resCh := make(chan applyResult, 1)`.

Capacity one lets `Reader` publish one result even after `waitApply` has left. The reader can then continue consuming `applyCh`. A second 1,000-round run after the change passed every round.

> [!BUG]
> **Symptom:** `TestRestartSubmit4A` reported that only two of three servers reached counter value 102.
>
> **Root cause:** an abandoned unbuffered result channel blocked the only `Reader`, even though Raft replication continued.
>
> **Fix:** use a capacity-one result channel so apply progress does not depend on a live waiter.
>
> **Verification:** `./test_lab4.sh 4A 1000 20` completed 1,000/1,000 rounds with `-race`.

---

## 6. What the 4A Tests Establish

| Test | Main behavior exercised |
|---|---|
| `TestBasic4A` | Replicate and execute sequential increments on all replicas |
| `TestConcurrent4A` | Serialize 50 concurrent submissions through Raft order |
| `TestLeaderFailure4A` | Continue in the majority and catch up the old Leader |
| `TestLeaderPartition4A` | Do not complete minority commands; finish after healing |
| `TestRestartReplay4A` | Rebuild service state by replaying persisted committed logs |
| `TestShutdown4A` | Stop outstanding submissions after Raft shutdown |
| `TestRestartSubmit4A` | Separate pre-restart waiters from post-restart chosen commands |

The tests cover the lab's crash, restart, partition, concurrency, and leadership-change model. They do not model Byzantine nodes, real disks, production process supervision, or unbounded resource usage.

---

## 7. Results

> [!SUCCESS]
> The final implementation passed 1,000 complete Lab 4A rounds. Every one of the seven test cases passed 1,000 times, for 7,000 successful test-case executions under the race detector.

### 7.1 Command

```bash
cd /home/zaxy/studyspace/projects/6.5840/src
./test_lab4.sh 4A 1000 20
```

The runner built `rsm1d` with `-race`, ran at most 20 complete 4A jobs concurrently, and aggregated each test case separately.

### 7.2 Result Dashboard

The following block is the exact final runner output committed in `rsm_4A_pass.log` by `c097b70`.

<!-- evidence: kind=log path=%5C%5Cwsl.localhost%5CUbuntu%5Chome%5Czaxy%5Cstudyspace%5Cprojects%5C6.5840%5Csrc%5Crsm_4A_pass.log lines=2012-2031 sha256=3e1b6e1b898abd01835cfe322bb02553186e97d4fcbeae200563fdd84c25432a -->

```text
Test case statistics
Suite  Test case                                    Avg time   Peers   Avg RPCs    Avg Ops   Pass   Fail   Runs
------ ------------------------------------------ ---------- ------- ---------- ---------- ------ ------ ------
4A     TestBasic4A                                    2.367s       3       44.0       10.0   1000      0   1000
4A     TestConcurrent4A                               1.014s       3       29.9       50.0   1000      0   1000
4A     TestLeaderFailure4A                            1.838s       3       31.1        2.0   1000      0   1000
4A     TestLeaderPartition4A                          3.073s       3      141.9        2.0   1000      0   1000
4A     TestRestartReplay4A                           18.757s       3      424.1      101.0   1000      0   1000
4A     TestShutdown4A                                10.663s       3        0.3        0.0   1000      0   1000
4A     TestRestartSubmit4A                           29.753s       3      436.7      102.0   1000      0   1000

Invocation summary
  Invocation timestamp: 20260814210857
  Summary log path: /home/zaxy/studyspace/projects/6.5840/src/test_log/lab4/20260814/4A_1000_20_default_20260814210857.log
  Debug log root/date: /home/zaxy/studyspace/projects/6.5840/src/debug_log/lab4/<job-start-date>
  Total jobs: 1000
  Passed jobs: 1000
  Failed jobs: 0
  4A rounds: passed=1000 failed=0; average time sum=67.465s
  Retained failure logs: none
```

`Avg time`, `Avg RPCs`, and `Avg Ops` are arithmetic means over 1,000 samples per test, calculated by `test_lab4.sh`. The average-time sum is the sum of the seven per-test average durations, not wall-clock duration and not throughput.

### 7.3 Limits of the Evidence

- The 1,000 rounds are strong evidence under the supplied tester and race detector, not a formal proof.
- The rounds ran concurrently, so timing values include host contention and should not be used as latency benchmarks.
- The committed summary contains derived averages; successful raw logs were deleted by the runner's default policy.
- `analyze_lab_logs.py` did not recognize the custom runner summary format, so the table above is retained as exact runner output rather than re-labeled tool output.
- An abandoned waiter remains in `opResChs` until its index is eventually applied. The buffered channel prevents apply deadlock, but explicit waiter cleanup would better bound volatile state.
- The `CommandValid == false` branch is not yet suitable for Lab 4C snapshot messages because it assumes `msg.Command` is an `Op`.

---

## 8. Conclusion

Lab 4A is a small layer with a demanding concurrency contract. `Submit` and `Reader` meet at a Raft log index, but success is decided by the operation identity actually chosen there. One ordered reader preserves state-machine order, and a capacity-one result channel prevents a departed waiter from stopping all later apply progress. The strongest evidence is the final 1,000/1,000 race-enabled run. The next work for Lab 4B and 4C is to add client retry/deduplication and snapshot-aware apply handling without weakening these invariants.
