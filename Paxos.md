6.5840 2026 Lecture 4: Fault-Tolerant Agreement, Paxos

From Paxos Made Simple, by Leslie Lamport, 2001

remember GFS coordinator
  what if the coordinator fails?
  GFS paper did not have much to say about what happens
  many services built on GFS, so ought to be automatic and reliable
  what to do?

good overall goals:
  high availability via replication
    continue even if one server has crashed or can't be contacted
    "no single point of failure"
  strongly consistent
    look as if a single server
    
why designing a fault-tolerant coordinator is hard
  suppose we want to use primary/backup replication
    we want clients to switch to backup if primary fails
  a broken scheme:
  \[C1, C2, net, S1, S2\]
  clients ordinarily send all ops to S1
    S1 replies, and also forwards to S2 (for replication)
    S1 is primary, S2 is backup
  if client gets no response from S1, re-send to S2
    so the system can tolerate the failure of S1
  problem:
    suppose network failure:
      C1 and S1 can communicate;
      C2 and S2 can communicate;
      but C1/S1 can't talk to C2/S2
    on both sides, it seems like the other failed
    so both S1 and S2 will independently act as primary
    "network partition"
    "split brain"
  computers can't distinguish "server crashed" from "network broken"
    all they can observe is "no response to my request"
  this comes up again and again in fault-tolerant designs!

for a while automated fault-tolerant fail-over seemed impossible
  due to possibility of partition
  special external agent (a human) was used to switch primaries
  but that's a single point of failure
  not very scalable for big systems

around 1989 a few solutions appeared -- surprise!
  Paxos is the simplest, easiest to understand
  inspired many widely used schemes
    we'll see Paxos in multiple papers later in the course
  incorporates ideas worth knowing

Paxos provides fault-tolerant "agreement", "consensus"
  to allow a set of computers to agree on a single value
  for example, who the current primary is
  if some Paxos participants agree on X,
    no participant will think some other value was agreed
  despite computer or network failures
  no split brain even if partition

Paxos performs just one agreement
  Real systems need a sequence of Paxos agreements
    e.g. a new agreement to recover after each coordinator failure

a key idea: quorums
  \[diagram: three servers\]
  an odd number of servers, e.g. 3
  responses from a majority are required to do anything -- 2 out of 3
  if cannot contact a majority, cannot make progress
  (quorum idea is much older than Paxos)

a useful property of majorities is that any two intersect
  if some information is known by a majority,
    any later majority will contain at least one server that knows
  so each step in a quorum system typically looks like
    send message to all participants
    gather a majority of responses (but don't wait for more)
      look at responses to find latest state
    send updated state message to all participants
  nice:
    tolerates minority of failed or slow servers
    overlap ensures state changes aren't lost

another property of majorities:
  at most one network partition can contain a majority of servers
  servers in the minority partitions won't get a quorum, won't proceed
  so at most one partition will perform operations
    helps avoid split brain

note: majority is out of all servers, not just out of live ones

we say 2f+1 servers can tolerate f failed servers
  since the remaining f+1 is a majority of 2f+1
  3 servers can tolerate 1 failed server
  5 servers can tolerate 2 failed
  so you can get more availability (at some expense!)
  
What properties does Paxos guarantee?
  correctness:
    if agreement reached, all agreeing servers agree on same value
    once any agreement reached, never changes its mind
  fault-tolerance:
    can proceed if f+1 of 2f+1 (a majority) can communicate
      i.e. can tolerate <= f failed servers
    no majority -> cannot proceed
      but will resume correctly once f+1 are again available
      e.g. can survive a site-wide power failure
  liveness:
    will reach agreement when a majority can communicate for long enough
    (this is a weak property)

let's look at the Paxos algorithm
  this is a pseudo-code version of the paper

```txt
  --- Paxos Proposer ---
	
	proposer(v):
    while not decided:
	    choose n, unique and higher than any n seen so far
	    send prepare(n) to all servers including self
	    if prepare_ok(n, na, va) from majority:
	      v' = va with highest na; choose own v otherwise   
	      send accept(n, v') to all
	      if accept_ok(n) from majority:
	        send decided(v') to all
	
  
  --- Paxos Acceptor ---

	state on each node (persistent):
	 np     --- highest prepare seen
	 na, va --- highest accept seen
	
	prepare(n) handler:
	 if n > np
	   np = n
	   reply prepare_ok(n, na, va)
   else
     reply reject
	
	
	accept(n, v) handler:
	 if n >= np
	   np = n
	   na = n
	   va = v
	   reply accept_ok(n)
   else
     reply reject
```

to cope with failures during agreement protocol,
  Paxos may go through multiple numbered rounds,
    each driven by a proposer.
  any server can propose if it thinks agreement hasn't yet been reached.
  two message exchanges per round:
    prepare, prepare_ok
    accept, accept_ok

definition: S accepts n/v
  S responded accept_ok to accept(n, v)

definition: n/v is chosen
  a majority accepted n/v

the crucial property:
  if v is chosen, any subsequent choice will also == v
    protocol will not change its mind
    maybe a different proposer &c, but same value!
  tricky b/c "chosen" is system-wide property
    e.g. what if majority accepts, then proposer crashes?
    no server can tell locally that a value was chosen

----------------

example 1 (normal operation):
  S1, S2, S3
  but S3 is dead or slow
  S1 starts proposal, n=5 v=A
S1: p5    a5A    dA
S2: p5    a5A    dA
S3: dead...
"p5" means Sx receives prepare(n=5) 
"a5A" means Sx receives accept(n=5, v=A)
"dA" means Sx receives decided(v=A)
these diagrams are not specific about who the proposer is
  we only care about what acceptors saw and replied

Proposer only needs to wait for a majority
  so it can continue even though S3 was down

What would happen if S3 was alive, but network partitioned?
  and S3 wanted to propose value B?
  S3's prepare would not assemble a majority

the homework question:
  How does Paxos ensure that the following sequence of events can't
  happen? What actually happens, and which value is ultimately chosen?
  S1 wants to propose v=X, crashes after sending two accepts
  S2 wants v=Y
  S1: p1 a1X
  S2: p1     p2 a2?
  S3: p1 a1X p2 a2?
  S3's prepare_ok to prepare(2) would have included "X"
    thus a2X
    good: X had been chosen in the first round; chosen again in 2nd
  the point:
    if the system has already reached agreement, majority will know value.
    any new majority of prepares will intersect that majority.
    so subsequent proposer will learn of already-agreed-on value,
    and send it in accept msgs

example 2 (concurrent proposers):
S1 starts proposing n=10
S1 sends first accept v=X; meanwhile
S3 starts proposing n=11
  but S1 does not receive its proposal
  S3 only has to wait for a majority of prepare responses
S1: p10 a10X
S2: p10        p11
S3: p10        p11  a11Y
S1 is still sending out accept messages...
has a value been chosen?
what will happen?
  what will S2 do if it gets a10X accept msg from S1?
  what will S1 do if it gets a11Y accept msg from S3?
what if S3 were to crash after just one a11Y (and not restart)?

important design pattern:
  if there's evidence that agreement *might* have been reached already,
    must act as if it had been.
  proposer may not *know*,
    since its majority may overlap by just one with previous.
  nevertheless, must be conservative.

how about this:
S1: p10  a10X               p12
S2: p10          p11  a11Y  
S3: p10          p11        p12   a12X
has the system agreed to a value at this point?
  after all, a majority have accepted value "X"

what's the "commit point"?
  i.e. exactly when has agreement been reached?
  i.e. at what point can a server safely act on the agreement?
  i.e. at what point might *some other* server have already acted?
  after a majority has the same v_a? no -- why not?  above counterexample
  after a majority has the same v_a/n_a? yes

why does the proposer need to pick v_a with highest n_a?
S1: p10  a10A               p12
S2: p10          p11  a11B  
S3: p10          p11  a11B  p12   a12?
n=11 already agreed on vB
n=12 sees both vA and vB, but must choose vB
why this makes sense: two cases:
  1. a value had been chosen before n=11
     n=11's prepares would have seen value and re-used it
     so it's safe for n=12 to re-use n=11's value
  2. no value had been chosen before n=11
     n=11 might have obtained a majority
     so it's required for n=12 to re-use n=11's value

why does accept handler check n >= n_p?
  w/o n >= n_p check, you could get this bad scenario:
  S1: p1 p2 a1A
  S2: p1 p2 a1A a2B
  S3: p1 p2     a2B
  oops, for a while A was chosen, then changed to B!

why does accept handler update n_p = n?
  required to prevent earlier n's from being accepted
  server can receive accept(n,v) even though it never saw prepare(n)
  without n_p = n, can get this bad scenario:
  S1: p1    a2B a1A p3 a3A
  S2: p1 p2         p3 a3A
  S3:    p2 a2B
  oops, for a while B was chosen, then changed to A!

what if proposer S2 chooses n < S1's n?
  e.g. S2 didn't see any of S1's messages
  S2 won't make progress, so no correctness problem

what if an acceptor crashes+reboots after sending accept_ok?
S1: p1  a1X
S2: p1  a1X reboot  p2  a2?
S3: p1              p2  a2?
we know X was chosen; but will proposal #2 preserve X?
the story:
  S2 is the only intersection between p1's and p2's majorities
  thus the only evidence that Paxos already chose X
  so S2 *must* return X in prepare_ok(n=2)
  so S2 must be able to recover its pre-crash n_a/v_a (and n_p)
thus: if S2 wants to re-join this Paxos instance after crash,
  it must remember its n_p/v_a/n_a on disk.
  if lost, do not re-join!

what if an acceptor reboots after sending prepare_ok?
  does it have to remember n_p on disk?
  if n_p not remembered, this could happen:
  S1: p10            a10X
  S2: p10 p11 reboot a10X a11Y
  S3:     p11             a11Y
  11's did not receive a prepare_ok w/ X, so 11 proposed its own value Y
  but just before that, X had been chosen!
  b/c S2 did not remember to ignore a10X
  so: persisting n_p prevents this

can Paxos get stuck?
  yes, if there is not a majority that can communicate
  once net/computers repaired, Paxos will continue

Performance?
  viewed as slow due to multiple communication rounds
    but lots of schemes for optimizing/batching
  viewed as expensive: need 3 replicas (not 2)
  but hard to avoid in serious fault-tolerant systems

-----------------

Many systems use Paxos internally, in a couple of styles.
  To agree on a new coordinator when the old one doesn't respond.
  To agree on when the backup should take over from the primary.
  To agree on the sequence of operations in an RSM.

How to build a database RSM using Paxos
  \[diagram: clients, 3 replicas, paxos layer, log, DB layer\]
  clients can send operations to any replica
  servers use Paxos to *agree* on each next op to append to the log
  servers apply agreed+logged operations to state (e.g. k/v DB)
    in log order

example:
  client sends "x=1" to S1
  S1 picks log entry 3
  S1 uses Paxos to get all servers to agree that entry 3 holds x=1
  after agreement ("commit"), all DBs execute x=1
  
example:
  client sends "x?" to S2
  S2 picks log entry 4
  S2 uses Paxos to get all servers to agree that entry 4 holds "x?"
  after agreement, S2's DB executes "x?"
    and replies to the client
  
why a log?
  help cope with concurrency
    need to hold later operations until earlier ones have committed
  help replicas catch up
    not in majority, briefly offline, &c
  usually only the tail of the log is kept, along with "state"

next:
  guest lecture by a Go wizard
  then Raft
  Lab 1 due tomorrow