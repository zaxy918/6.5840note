# MIT 6.5840 Lecture 4: Fault-Tolerant Agreement and Paxos

> Based on MIT 6.5840 Lecture 4 slides: **Fault-Tolerant Agreement, Paxos**  
> Main reference: Leslie Lamport, *Paxos Made Simple* (2001)  
> Course context: Paxos appears right after GFS and before the Go patterns / Raft lectures. It introduces the core problem that Raft later makes easier to understand: **how can a group of machines agree on one value despite failures?**

---

## 0. The Problem Paxos Solves

In the GFS paper, there is a coordinator-like component called the master. That raises an obvious question:

> What happens if the coordinator fails?

If the coordinator is a single machine, it becomes a single point of failure. Many systems are built on top of storage systems like GFS, so relying on a human to manually repair or replace the coordinator is not a satisfying design.

The broad goals are:

- **High availability**: the system should continue working even if one server crashes or cannot be contacted.
- **No single point of failure**: no one machine should be essential for progress.
- **Strong consistency**: the replicated system should behave like a single server.

Paxos is a protocol for solving this kind of problem.

More specifically, Paxos solves **consensus**:

> A group of computers must agree on one value, such as who the current primary is.

If some participants agree on value `X`, then no participant should later decide that some different value `Y` was agreed.

---

## 1. Why Fault-Tolerant Failover Is Hard

Suppose we try to build a simple primary/backup service.

```text
Clients: C1, C2
Servers: S1, S2

Normal case:
  S1 is primary.
  S2 is backup.
  Clients send operations to S1.
  S1 executes operations and forwards them to S2.
```

This sounds reasonable. If S1 fails, clients can send requests to S2.

But now consider a network partition:

```text
C1 can talk to S1.
C2 can talk to S2.
C1/S1 cannot talk to C2/S2.
```

From S1's perspective, S2 may look dead.  
From S2's perspective, S1 may look dead.

Both sides may decide:

```text
"The other side failed. I should be primary."
```

Now the system has two primaries. This is called **split brain**.

The deeper problem is:

> A computer cannot reliably distinguish a crashed server from a network problem. All it observes is lack of response.

This problem appears repeatedly in fault-tolerant distributed systems.

---

## 2. The Key Idea: Quorums

Paxos relies on **quorums**, usually majorities.

For example, with 3 servers:

```text
S1, S2, S3
```

a majority is any 2 of them.

With 5 servers, a majority is any 3.

The important property is:

> Any two majorities intersect.

For 3 servers:

```text
Majority A: S1, S2
Majority B: S2, S3
Intersection: S2
```

For 5 servers, any two groups of 3 must share at least one server.

This intersection property is the heart of Paxos safety.

If some information is known by a majority, then any later majority must contain at least one server that may know that information.

### Majority Across All Servers

The majority is counted out of all configured servers, not just the servers currently alive.

If there are 3 servers, a majority is always 2. If only one is reachable, the system cannot make progress.

This is intentional. If two disconnected partitions could each make progress, split brain would be possible.

### Fault Tolerance

With `2f + 1` servers, Paxos can tolerate `f` failed servers:

```text
3 servers tolerate 1 failure.
5 servers tolerate 2 failures.
7 servers tolerate 3 failures.
```

The remaining `f + 1` servers form a majority.

---

## 3. What Paxos Guarantees

Paxos provides three broad properties.

### Correctness / Safety

If agreement is reached, all agreeing participants agree on the same value.

Once a value is chosen, Paxos never changes its mind.

This is the most important property.

### Fault Tolerance

Paxos can proceed if a majority of servers can communicate.

If no majority can communicate, Paxos cannot make progress. But once a majority is restored, the protocol can resume correctly.

This means Paxos can survive temporary failures, including power failures, as long as necessary state is persisted.

### Liveness

Paxos will eventually reach agreement if a majority can communicate for long enough and proposers stop interfering with each other.

This is a weaker property than safety. Paxos is mainly famous for its safety guarantee.

---

## 4. Roles in Paxos

The lecture pseudocode focuses on two main roles:

- **Proposer**: tries to get a value chosen.
- **Acceptor**: receives prepare and accept requests, and records promises/accepted values.

In many real systems, the same machine plays multiple roles.

Paxos performs one agreement:

```text
Choose one value.
```

Real systems usually need a sequence of agreements:

```text
Choose log entry 1.
Choose log entry 2.
Choose log entry 3.
...
```

That extended use is called Multi-Paxos or a replicated state machine design.

---

## 5. Paxos State

Each acceptor stores persistent state:

```text
np      highest prepare number seen
na      highest accepted proposal number
va      value accepted with proposal number na
```

Meaning:

- `np`: the acceptor has promised not to accept proposals numbered lower than `np`.
- `na`: the number of the highest proposal this acceptor has accepted.
- `va`: the value accepted in that highest accepted proposal.

These values must survive crashes if the acceptor wants to rejoin the same Paxos instance.

---

## 6. Paxos Algorithm

Paxos proceeds in numbered rounds. Each round has a proposal number `n`, and proposal numbers must be unique and increasing.

A proposer tries to choose a value `v`.

### Phase 1: Prepare

The proposer chooses a proposal number:

```text
n = unique number higher than any number it has used before
```

Then it sends:

```text
prepare(n)
```

to all acceptors.

An acceptor handles `prepare(n)` like this:

```text
if n > np:
    np = n
    reply prepare_ok(n, na, va)
else:
    reply reject
```

The acceptor promises:

> I will not accept proposals numbered less than `n`.

It also tells the proposer the highest proposal/value it has already accepted.

### Phase 2: Accept

If the proposer receives `prepare_ok` from a majority, it chooses the value to propose.

Rule:

```text
If any prepare_ok replies contain an accepted value,
choose the value with the highest accepted proposal number na.

Otherwise, choose the proposer's own original value.
```

Then the proposer sends:

```text
accept(n, v')
```

to all acceptors.

An acceptor handles `accept(n, v)` like this:

```text
if n >= np:
    np = n
    na = n
    va = v
    reply accept_ok(n)
else:
    reply reject
```

If a majority replies `accept_ok`, then the value is **chosen**.

The proposer can then send:

```text
decided(v)
```

to let everyone learn the chosen value.

---

## 7. Important Definitions

### Accepted

An acceptor `S` has accepted proposal `n/v` if it replied:

```text
accept_ok(n)
```

to:

```text
accept(n, v)
```

### Chosen

A value `v` is chosen if a majority of acceptors accepted the same proposal:

```text
n/v
```

That is, the majority accepted the same proposal number and the same value.

### The Crucial Paxos Safety Property

> If value `v` is chosen, then every later chosen value must also be `v`.

The proposal number may change, but the value cannot change.

This is tricky because “chosen” is a global property. A majority may have accepted a value, but the proposer might crash before telling anyone. No single server may know for sure that the value was chosen.

Paxos is designed to preserve safety even under that uncertainty.

---

## 8. Normal Operation Example

Suppose there are three servers:

```text
S1, S2, S3
```

S3 is dead or slow.

S1 starts proposal number `5` with value `A`.

```text
S1: p5  a5A  dA
S2: p5  a5A  dA
S3: dead...
```

Notation:

- `p5` means the server received `prepare(5)`.
- `a5A` means the server received `accept(5, A)` and accepted it.
- `dA` means the server learned `decided(A)`.

The proposer only needs a majority, so S1 and S2 are enough.

If S3 is merely partitioned and tries to propose another value, it cannot reach a majority by itself. Therefore it cannot choose a conflicting value.

---

## 9. Why a New Proposer Must Preserve an Old Chosen Value

Consider:

```text
S1 wants value X.
S1 crashes after sending accepts to S1 and S3.
S2 later wants value Y.
```

Timeline:

```text
S1: p1  a1X
S2: p1       p2  a2?
S3: p1  a1X  p2  a2?
```

In round 1, value `X` was accepted by a majority:

```text
S1 and S3
```

So `X` was chosen, even if the proposer crashed before announcing it.

When S2 starts round 2, it sends `prepare(2)` to a majority. Any majority for round 2 must overlap with the old majority that accepted `X`.

In this example, S3 responds to `prepare(2)` with:

```text
na = 1
va = X
```

Therefore S2 must propose `X`, not its own value `Y`.

The lesson:

> If agreement might already have happened, a later proposer must act conservatively and preserve the possible chosen value.

---

## 10. Concurrent Proposers

Now consider two proposers acting at the same time.

```text
S1 starts proposal n=10 with value X.
S3 starts proposal n=11 with value Y.
```

Possible timeline:

```text
S1: p10  a10X
S2: p10        p11
S3: p10        p11  a11Y
```

Questions to ask:

- Has value `X` already been chosen?
- Has value `Y` already been chosen?
- What should acceptors do when they see old accept messages after newer prepares?

This example motivates two important rules:

1. A proposer must use the highest accepted value it hears about in prepare replies.
2. An acceptor must reject old accept requests after it has promised a higher proposal number.

Without these rules, Paxos could choose one value and later choose a different value.

---

## 11. What Is the Commit Point?

A tempting but wrong idea:

> Agreement has happened once a majority has accepted the same value.

This is not precise enough.

The correct commit point is:

> Agreement has happened when a majority has accepted the same proposal number and value: `n/v`.

Why does the proposal number matter?

Because acceptors may accept different proposal numbers over time. Seeing the same value on a majority is not enough unless it is tied to the same proposal instance.

Paxos safety is defined in terms of a majority accepting a specific numbered proposal.

---

## 12. Why Choose the Highest `na`?

During prepare, the proposer receives replies:

```text
prepare_ok(n, na, va)
```

It may hear about multiple previously accepted values. The rule is:

> Choose the value with the highest `na`.

Example:

```text
S1: p10  a10A               p12
S2: p10          p11  a11B
S3: p10          p11  a11B  p12  a12?
```

Proposal `11` may already have chosen value `B`.

When proposal `12` sees both:

```text
10/A
11/B
```

it must choose `B`, the value from the highest accepted proposal number.

Why?

There are two cases:

1. A value was chosen before proposal 11. Then proposal 11 would have been forced to reuse it.
2. No value was chosen before proposal 11. Then proposal 11 itself might have chosen `B`.

Either way, reusing the value from the highest `na` is the safe conservative choice.

---

## 13. Why Acceptors Check `n >= np`

The accept handler accepts `accept(n, v)` only if:

```text
n >= np
```

Without this check, an acceptor could accept an old proposal after promising a newer one.

Bad scenario without the check:

```text
S1: p1  p2  a1A
S2: p1  p2  a1A  a2B
S3: p1  p2        a2B
```

For a while, `A` could be chosen. Later, `B` could be chosen. That violates Paxos safety.

The check prevents old proposals from being accepted after newer promises.

---

## 14. Why Accept Updates `np = n`

The accept handler also does:

```text
np = n
na = n
va = v
```

Why update `np` during accept?

Because a server can receive `accept(n, v)` even if it did not previously see `prepare(n)`.

If accepting proposal `n` did not also raise `np`, the acceptor might later accept an older proposal. That could allow a value to be chosen and then replaced by another value.

The lecture gives a bad scenario:

```text
S1: p1     a2B  a1A  p3  a3A
S2: p1  p2           p3  a3A
S3:     p2  a2B
```

Without updating `np`, value `B` could be chosen and later `A` could be chosen. That is not allowed.

---

## 15. What If a Proposer Uses a Lower Number?

Suppose proposer S2 chooses a proposal number lower than a number already seen by other acceptors.

Then its prepare or accept requests will be rejected.

This may prevent progress for that proposer, but it does not break correctness.

That is an important Paxos theme:

> Paxos is willing to sacrifice progress temporarily in order to preserve safety.

---

## 16. Persistent State and Crashes

Persistent state is essential.

Suppose an acceptor accepts a value and then crashes.

```text
S1: p1  a1X
S2: p1  a1X  reboot  p2  a2?
S3: p1                p2  a2?
```

Value `X` was chosen by S1 and S2.

Later, proposal 2 uses a majority that includes S2 and S3. The only overlap with the old majority may be S2.

Therefore S2 must remember that it accepted `X`. If S2 forgets `na/va`, the new proposer may choose a different value.

So an acceptor must persist:

```text
np
na
va
```

If it loses this state, it must not rejoin the same Paxos instance as if nothing happened.

### Reboot After Prepare

Even `np` must be persistent.

Suppose an acceptor promised proposal 11, then rebooted and forgot that promise. It might later accept an older proposal 10, allowing a previously rejected old proposal to interfere with safety.

The lecture's bad scenario:

```text
S1: p10             a10X
S2: p10  p11 reboot a10X  a11Y
S3:      p11              a11Y
```

Forgetting `np` lets S2 accept `a10X` even though it had promised not to accept proposal 10 anymore. This can violate safety.

---

## 17. Can Paxos Get Stuck?

Yes.

If no majority can communicate, Paxos cannot make progress.

Example with 3 servers:

```text
S1 is isolated.
S2 is isolated.
S3 is isolated.
```

No server can obtain a majority of 2, so no value can be chosen.

But once a majority can communicate again, Paxos can continue safely.

This is the safety/liveness tradeoff:

```text
No majority -> no progress.
But also no split brain.
```

---

## 18. Performance

Paxos has a reputation for being expensive because:

- it requires multiple communication rounds,
- it usually needs at least 3 replicas to tolerate 1 failure,
- it requires persistent state,
- repeated single-value Paxos instances can be costly.

However, serious fault-tolerant systems often cannot avoid these costs entirely.

Real systems optimize Paxos with:

- stable leaders,
- batching,
- pipelining,
- Multi-Paxos,
- log compaction.

Raft, which the course covers next, can be seen as a more understandable protocol for the same replicated-state-machine setting.

---

## 19. How Paxos Is Used in Real Systems

Paxos is usually used internally, not exposed to users directly.

Common uses:

- agree on the current coordinator,
- agree when a backup should take over from a primary,
- agree on the next operation in a replicated log,
- build a replicated state machine.

---

## 20. Building a Replicated State Machine with Paxos

A replicated state machine keeps several replicas in the same state by ensuring they execute the same operations in the same order.

Architecture:

```text
Clients
   |
   v
Replicas
   |
   v
Paxos layer agrees on log entries
   |
   v
Replicated log
   |
   v
State machine, e.g. key/value DB
```

Each log entry is chosen using Paxos.

Example:

```text
Client sends "x = 1" to S1.
S1 proposes that log entry 3 should contain "x = 1".
Paxos chooses that value for entry 3.
All replicas execute entry 3.
```

Another example:

```text
Client sends "read x" to S2.
S2 proposes that log entry 4 should contain "read x".
Paxos chooses that value for entry 4.
S2 executes the operation in log order and replies to the client.
```

Even reads may need to go through the log if the system wants strong consistency.

### Why a Log?

The log helps with:

- ordering concurrent operations,
- ensuring all replicas execute the same operations,
- holding later operations until earlier ones commit,
- letting offline replicas catch up,
- retaining only a compacted state plus the tail of the log over time.

This is exactly the replicated state machine idea that appears again in Raft.

---

## 21. The Most Important Paxos Intuition

The most important Paxos idea is:

> If a value might already have been chosen, future proposers must preserve it.

Because majorities intersect, a future proposer can learn about possible past choices through prepare replies.

The highest accepted proposal number in those replies tells the proposer which value is safest to continue with.

Paxos is conservative:

```text
It does not need to know for certain that a value was chosen.
If the value might have been chosen, Paxos acts as if it was.
```

That conservatism is what prevents split brain.

---

## 22. A Compact Mental Model

Think of Paxos as a two-step safety filter.

### Prepare Phase

The proposer asks:

```text
"Has anyone seen evidence of a value that might already have been chosen?"
```

Acceptors respond with their highest accepted proposal/value.

### Accept Phase

The proposer says:

```text
"Given what I learned, here is the only value I am allowed to try now."
```

If a majority accepts it, the value is chosen.

---

## 23. Common Misunderstandings

### Misunderstanding 1: Two Replicas Are Enough

Two replicas cannot safely tolerate one failure with automatic failover.

If the two replicas cannot communicate, each side cannot know whether the other crashed or the network partitioned. A majority system with 3 replicas avoids this by requiring 2 votes.

### Misunderstanding 2: The Proposer Can Always Choose Its Own Value

The proposer can choose its own value only if the majority of prepare replies reports no previously accepted value.

If it hears about an accepted value, it may be forced to reuse that value.

### Misunderstanding 3: Accepting a Value Means Knowing It Is Chosen

An acceptor may accept a value without knowing whether a majority accepted it.

Chosen is a system-wide fact.

### Misunderstanding 4: Timeout Means Failure

A missing response may mean:

- the server crashed,
- the network dropped the message,
- the response was delayed,
- the server is slow,
- the reply was lost.

Paxos is designed around this uncertainty.

---

## 24. Final Summary

Paxos solves the problem of fault-tolerant agreement.

It prevents split brain by requiring majorities, and it preserves safety through the intersection of majorities.

The prepare phase discovers whether some value might already have been chosen. The accept phase tries to choose a value that is consistent with that history.

The key safety rule is:

> Once a value is chosen, every later chosen value must be the same value.

This rule holds even if:

- proposers crash,
- acceptors crash and reboot with persistent state,
- messages are delayed,
- proposals overlap,
- network partitions occur.

Paxos may stop making progress without a majority, but it will not choose conflicting values.

That is the fundamental tradeoff:

```text
No quorum, no progress.
But also no split brain.
```

---

## One-Sentence Summary

> Paxos uses intersecting majorities and conservative proposal rules to ensure that a distributed system can choose at most one value, even when machines crash, messages are lost, and no server can locally know for sure what has already been chosen.

