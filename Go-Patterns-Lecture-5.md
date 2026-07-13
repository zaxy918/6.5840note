# MIT 6.5840 Lecture 5: Go Patterns

> Source slides: [Patterns and Hints for Concurrency in Go](https://pdos.csail.mit.edu/6.824/notes/Go-MIT6824-2026.pdf)  
> Course schedule: [MIT 6.5840 Schedule](https://pdos.csail.mit.edu/6.824/schedule.html)  
> Lecturer: Russ Cox  
> Context: This lecture appears before the Raft labs because the rest of the course relies heavily on careful Go concurrency.

---

## 0. Why This Lecture Matters

This lecture is not mainly about Go syntax. It is about how to structure concurrent programs before you start writing systems such as Raft, key/value servers, and replicated services.

The central message is:

> Go concurrency is not about sprinkling `go` everywhere. It is about giving each concurrent activity a clear responsibility, a clear communication path, and a clear exit condition.

In MIT 6.5840, most hard bugs are not ordinary syntax bugs. They are bugs such as:

- a goroutine that never exits,
- a channel send with no receiver,
- a lock held while doing a blocking RPC,
- a timeout that returns while an old goroutine is still alive,
- a stale RPC reply that modifies current state,
- a data race on shared state.

This lecture prepares you for those problems.

---

## 1. Concurrency Is Not Parallelism

### The Basic Difference

**Concurrency** means structuring a program so that it can deal with multiple things at once.

**Parallelism** means actually executing multiple computations at the same physical time, usually on multiple CPU cores.

A kitchen analogy helps:

- If one person starts boiling water, then chops vegetables while the water heats, then returns to the pot, that is concurrency.
- If two people cook at the same time, one chopping and one frying, that is parallelism.

So:

```text
Concurrency = program structure.
Parallelism = physical simultaneous execution.
```

In 6.5840, most labs are primarily about concurrency.

For example, a MapReduce coordinator must deal with:

- many workers asking for tasks,
- workers finishing tasks,
- workers timing out,
- the transition from map phase to reduce phase,
- final shutdown.

The main challenge is not using all CPU cores. The main challenge is making these activities interact correctly.

### Why This Lecture Comes Before Raft

Raft contains many concurrent activities:

- receiving `RequestVote` RPCs,
- receiving `AppendEntries` RPCs,
- election timeout logic,
- leader heartbeats,
- client commands,
- applying committed log entries to `applyCh`.

If these are not organized carefully, the implementation can easily deadlock or behave nondeterministically.

For example, this pattern is dangerous:

```go
rf.mu.Lock()
defer rf.mu.Unlock()

ok := rf.sendAppendEntries(server, args, reply)
```

The problem is that `sendAppendEntries` is an RPC. It may block. If the lock is held during that RPC, other goroutines cannot access Raft state. This can freeze the peer.

The safer pattern is:

```go
rf.mu.Lock()
args := buildArgsFromState()
term := rf.currentTerm
rf.mu.Unlock()

ok := rf.sendAppendEntries(server, args, reply)

rf.mu.Lock()
defer rf.mu.Unlock()

if !ok {
    return
}
if rf.currentTerm != term || rf.state != Leader {
    return
}

// Now process the reply.
```

The important idea is:

> Hold locks only while reading or modifying shared state. Do not hold locks while doing slow or blocking operations.

---

## 2. Core Go Concurrency Tools

### Goroutines

A goroutine is a lightweight concurrent execution path.

```go
go f()
```

means:

> Start running `f()` concurrently, while the current code continues.

A goroutine is like asking another person to do a task in the background. But you must know:

- what it is waiting for,
- when it finishes,
- what happens if the caller gives up,
- whether it can become stuck forever.

A common bug:

```go
func callWithTimeout() {
    ch := make(chan string)

    go func() {
        result := slowRPC()
        ch <- result
    }()

    select {
    case r := <-ch:
        fmt.Println(r)
    case <-time.After(time.Second):
        return
    }
}
```

If the timeout happens first, the function returns. Later, the goroutine may try:

```go
ch <- result
```

But nobody is receiving anymore. Since `ch` is unbuffered, the goroutine blocks forever. This is a goroutine leak.

A common fix is:

```go
ch := make(chan string, 1)
```

The buffer lets the late result be placed into the channel so the goroutine can exit.

### Channels

A channel is a communication path between goroutines.

```go
ch <- value   // send
value := <-ch // receive
```

An **unbuffered channel** is a direct handoff. The sender and receiver must meet at the same time.

```go
ch := make(chan int)
```

A **buffered channel** has limited storage.

```go
ch := make(chan int, 2)
```

The sender can send without blocking while the buffer has space. Once the buffer is full, sends block.

Buffered channels are often useful as queues, for example:

```go
idleWorkers := make(chan Worker, n)
```

This can represent a queue of idle workers.

### Mutexes

A mutex protects shared state.

```go
type Counter struct {
    mu sync.Mutex
    n  int
}

func (c *Counter) Inc() {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.n++
}
```

Use a mutex when many goroutines need short, direct access to the same shared data.

For example, a simple key/value server is usually clearest with:

```go
type KVServer struct {
    mu sync.Mutex
    db map[string]Entry
}
```

### Channel or Mutex?

There is no universal winner.

Use a mutex when:

- there is shared state,
- operations are short,
- direct access is simplest.

Use channels when:

- you are passing ownership of work,
- you are modeling queues,
- you are sending events,
- one goroutine should own a piece of state.

The Go slogan:

> Do not communicate by sharing memory; share memory by communicating.

does not mean “never use mutexes.” It means that when ownership can naturally move through communication, channels are often clearer. But when shared state is simple, a mutex may be better.

---

## 3. Goroutines for State

One important pattern in the slides is:

> A goroutine can hold state.

State does not always have to live in struct fields. It can also live in:

- local variables,
- the call stack,
- the current line where a goroutine is blocked.

### Explicit State Machine

Imagine a parser for quoted strings. A beginner might write:

```go
const (
    start = iota
    inString
    afterBackslash
    done
)

state := start

for {
    c := readChar()

    switch state {
    case start:
        if c != '"' {
            return false
        }
        state = inString

    case inString:
        if c == '"' {
            state = done
        } else if c == '\\' {
            state = afterBackslash
        }

    case afterBackslash:
        state = inString

    case done:
        return true
    }
}
```

This works, but the reader must constantly remember what each state means.

### State as Control Flow

The same logic can be written more naturally:

```go
func parse() bool {
    if readChar() != '"' {
        return false
    }

    for {
        c := readChar()

        if c == '"' {
            return true
        }

        if c == '\\' {
            readChar()
        }
    }
}
```

There is no explicit `state` variable, but the state still exists. The program’s position in the code represents the state.

### State Inside a Goroutine

Now suppose characters arrive one at a time from outside. A goroutine can own the parser state:

```go
type Parser struct {
    in chan byte
}

func NewParser() *Parser {
    p := &Parser{in: make(chan byte)}
    go p.loop()
    return p
}

func (p *Parser) Write(c byte) {
    p.in <- c
}

func (p *Parser) loop() {
    c := <-p.in
    if c != '"' {
        return
    }

    for {
        c := <-p.in
        if c == '"' {
            return
        }
        if c == '\\' {
            <-p.in
        }
    }
}
```

The parser state is represented by where `loop` is currently waiting:

- waiting for the first character,
- waiting inside the string,
- waiting for the escaped character after `\`.

This can make some programs much clearer.

### The Danger

If `loop` exits and someone later calls:

```go
p.in <- c
```

the sender may block forever because nobody is receiving.

So when using goroutines to hold state, always define:

- who creates the goroutine,
- who sends to it,
- who receives from it,
- how it exits,
- what happens after it exits.

---

## 4. Debugging Goroutines and Basic Discipline

Concurrent bugs often look like this:

- the test hangs,
- no panic occurs,
- no output appears,
- `Done()` never returns true,
- a test only fails sometimes.

### Goroutine Stack Dumps

When a Go program hangs, you often want to know:

- which goroutines exist,
- where each goroutine is blocked,
- who is waiting on a channel,
- who is waiting on a mutex,
- who is stuck in RPC or sleep.

On Unix-like systems, `Ctrl-\` sends `SIGQUIT`, causing Go to print all goroutine stacks.

Useful stack states include:

```text
[chan send]
[chan receive]
[sync.Mutex.Lock]
[semacquire]
[select]
[IO wait]
```

These tell you where the program is stuck.

### Every Goroutine Needs an Exit Condition

Bad pattern:

```go
go func() {
    for {
        time.Sleep(time.Second)
        check()
    }
}()
```

This goroutine never exits.

Better:

```go
func startChecker(done <-chan struct{}) {
    go func() {
        ticker := time.NewTicker(time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ticker.C:
                check()
            case <-done:
                return
            }
        }
    }()
}
```

Now the goroutine has a clear shutdown path.

### Every Channel Operation Needs a Counterparty

For every send:

```go
ch <- x
```

ask:

- who receives it?
- what if no one is receiving?
- what if the receiver already exited?

For every receive:

```go
x := <-ch
```

ask:

- who sends it?
- what if no one sends?
- what happens if the channel closes?

### Do Not Hold Locks While Blocking

Avoid doing these while holding a mutex:

- RPC calls,
- channel sends or receives,
- `time.Sleep`,
- long computation,
- waiting on a condition.

This is especially important in Raft.

### Channel Close Is Not a Universal Stop Signal

Closing a channel means:

> No more values will be sent on this channel.

It does not forcibly stop goroutines.

Usually, the sender closes the channel. If there are multiple senders, closing becomes delicate because sending on a closed channel panics.

### Use the Race Detector

Data races are common in distributed systems labs.

Run:

```bash
go test -race
```

If it reports a race, fix it before trusting your logic.

---

## 5. Pub/Sub Server Pattern

The first major example is a publish/subscribe server.

The API is roughly:

```go
Publish(e Event)
Subscribe(c chan<- Event)
Cancel(c chan<- Event)
```

The server keeps a set of subscribers. Every published event should be sent to the subscribers.

### Naive Mutex Version

```go
type Server struct {
    mu   sync.Mutex
    subs map[chan<- Event]bool
}

func (s *Server) Publish(e Event) {
    s.mu.Lock()
    defer s.mu.Unlock()

    for c := range s.subs {
        c <- e
    }
}
```

This is dangerous because:

```go
c <- e
```

may block. If a subscriber is slow, `Publish` blocks while holding the mutex. Then `Subscribe`, `Cancel`, and other `Publish` calls may all be blocked.

The lesson:

> Do not hold an important lock while sending on a channel that may block.

### Copy Then Send

One improvement is to copy the subscriber list while holding the lock, then send after unlocking:

```go
func (s *Server) Publish(e Event) {
    s.mu.Lock()
    subs := make([]chan<- Event, 0, len(s.subs))
    for c := range s.subs {
        subs = append(subs, c)
    }
    s.mu.Unlock()

    for _, c := range subs {
        c <- e
    }
}
```

This avoids holding the lock while sending. But now `Cancel` may close a channel after it was copied but before `Publish` sends to it. Sending on a closed channel panics.

So the real issue is not just syntax. The system must define clear semantics:

- what happens if a subscriber is slow?
- what happens if cancellation races with publishing?
- who closes subscriber channels?
- are old events delivered after cancellation?

### Server Loop Version

Another design is to make one goroutine own the subscriber map:

```go
type Server struct {
    publish   chan Event
    subscribe chan chan<- Event
    cancel    chan chan<- Event
}

func NewServer() *Server {
    s := &Server{
        publish:   make(chan Event),
        subscribe: make(chan chan<- Event),
        cancel:    make(chan chan<- Event),
    }
    go s.loop()
    return s
}

func (s *Server) loop() {
    subs := make(map[chan<- Event]bool)

    for {
        select {
        case e := <-s.publish:
            for c := range subs {
                c <- e
            }

        case c := <-s.subscribe:
            subs[c] = true

        case c := <-s.cancel:
            delete(subs, c)
            close(c)
        }
    }
}
```

Now the subscriber map is private to `loop`, so no mutex is needed.

But a slow subscriber can still block:

```go
c <- e
```

So channels do not magically solve slow-consumer problems.

### Main Lesson

The Pub/Sub example teaches:

- slow goroutines can affect the whole system,
- lock scope matters,
- channel close semantics matter,
- goroutine loops can own state,
- channel-based designs are not automatically safer than mutex designs.

---

## 6. Task Scheduler Pattern

This pattern is closest to the MapReduce lab.

The problem:

> Given many tasks and several workers, assign tasks to idle workers and wait until all tasks complete.

### Idle Worker Queue

A buffered channel can represent idle workers:

```go
idle := make(chan Worker, len(workers))

for _, w := range workers {
    idle <- w
}
```

To assign work:

```go
w := <-idle
```

If no worker is idle, this receive blocks.

When the worker finishes:

```go
idle <- w
```

The worker becomes available again.

### Simple Scheduler

```go
func Schedule(workers []Worker, tasks []Task) {
    idle := make(chan Worker, len(workers))
    done := make(chan bool)

    for _, w := range workers {
        idle <- w
    }

    for _, t := range tasks {
        w := <-idle

        go func(w Worker, t Task) {
            ok := call(w, t)
            if ok {
                done <- true
            }
            idle <- w
        }(w, t)
    }

    for range tasks {
        <-done
    }
}
```

This shows the pattern, but it is incomplete:

- if `call` fails, no `done` is sent,
- failed tasks are not retried,
- many tasks may create many goroutines,
- shutdown is not handled.

### Failure and Retry

Retry makes scheduling more complex.

If a task fails, it must be put back into the task queue. But sending failed tasks back into a channel can itself block if no goroutine is receiving.

The general lesson:

> Task scheduling looks simple until you add failure, retry, shutdown, and duplicate completion.

### Duplicate Completion

In MapReduce, a worker may be declared dead after a timeout, but later return and report completion.

Timeline:

```text
task 3 -> worker A
worker A times out
task 3 -> worker B
worker B finishes
worker A later reports done
```

The coordinator must not count task 3 twice.

So completion should be checked against task state and task ID.

---

## 7. Replicated Service Client Pattern

This pattern is about clients that can call multiple replicas.

The problem:

> There are several servers. Some may be down, slow, or not the leader. The client wants a successful response.

### Basic Retry Loop

```go
func (ck *Clerk) Call(args Args) Reply {
    for {
        for _, srv := range ck.servers {
            var reply Reply
            ok := srv.Call("Service.Op", &args, &reply)
            if ok && reply.Err == OK {
                return reply
            }
        }
        time.Sleep(100 * time.Millisecond)
    }
}
```

This works in simple cases, but a slow server can delay the client.

### Timeout Wrapper

```go
type result struct {
    reply Reply
    ok    bool
}

func callWithTimeout(srv Server, args Args) (Reply, bool) {
    done := make(chan result, 1)

    go func() {
        var reply Reply
        ok := srv.Call("Service.Op", &args, &reply)
        done <- result{reply: reply, ok: ok}
    }()

    select {
    case r := <-done:
        return r.reply, r.ok
    case <-time.After(100 * time.Millisecond):
        return Reply{}, false
    }
}
```

The channel is buffered so a late RPC result does not block the goroutine forever.

### Preferred Server

Clients often remember the last successful server:

```go
type Clerk struct {
    mu      sync.Mutex
    servers []Server
    prefer  int
}
```

Then calls start from `prefer`:

```go
start := ck.prefer
for i := 0; i < len(ck.servers); i++ {
    si := (start + i) % len(ck.servers)
    // try server si
}
```

This is useful because leaders tend to remain leaders for a while.

### Main Lessons

- timeout means “stop waiting,” not “the operation did not happen,”
- a late RPC result may still arrive,
- result channels often need buffering,
- do not hold a mutex while doing RPC,
- remember the likely leader or preferred server,
- retry logic must account for uncertainty.

This connects directly to `ErrMaybe`: if a request may have been executed but the reply was lost, the client cannot safely assume failure.

---

## 8. Protocol Multiplexer Pattern

This pattern handles multiple concurrent calls over one underlying connection.

The problem:

> Many goroutines want to call concurrently, but the underlying connection only supports one `Send` and one `Recv` stream.

Replies may arrive out of order, so each request needs an ID.

### Request Tags

```go
type Request struct {
    Tag  int
    Args Args
}

type Reply struct {
    Tag   int
    Value string
    Err   error
}
```

The tag connects a reply to its original request.

### Client Structure

```go
type Client struct {
    conn Conn

    sendCh chan Request

    mu      sync.Mutex
    nextTag int
    pending map[int]chan Reply
}
```

The key data structure is:

```text
pending[tag] = channel waiting for that reply
```

### Send and Receive Loops

```go
func (c *Client) sendLoop() {
    for req := range c.sendCh {
        c.conn.Send(req)
    }
}
```

Only `sendLoop` calls `Send`.

```go
func (c *Client) recvLoop() {
    for {
        reply, err := c.conn.Recv()
        if err != nil {
            c.failAll(err)
            return
        }

        c.mu.Lock()
        done := c.pending[reply.Tag]
        delete(c.pending, reply.Tag)
        c.mu.Unlock()

        if done != nil {
            done <- reply
        }
    }
}
```

Only `recvLoop` calls `Recv`. It uses the reply tag to wake the correct caller.

### Call

```go
func (c *Client) Call(args Args) (Reply, error) {
    done := make(chan Reply, 1)

    c.mu.Lock()
    tag := c.nextTag
    c.nextTag++
    c.pending[tag] = done
    c.mu.Unlock()

    c.sendCh <- Request{Tag: tag, Args: args}

    reply := <-done
    return reply, reply.Err
}
```

With timeout, `Call` must remove the entry from `pending` to avoid memory leaks.

### Main Lessons

- concurrent callers should not directly share a fragile underlying connection,
- serialize sends through one loop,
- serialize receives through one loop,
- tag requests,
- use a pending map,
- clean up on timeout,
- wake all pending callers if the connection fails,
- ignore late or unknown replies.

---

## 9. Final 6.5840 Lab Applications

### MapReduce

MapReduce is a task scheduler problem:

- tasks need IDs,
- task state must be tracked,
- workers may time out,
- timed-out workers may later return,
- duplicate completion must not be counted twice,
- map phase must finish before reduce phase starts.

A clear mutex-based coordinator is often simpler than a channel-heavy design.

### Key/Value Server and Lock

For a simple key/value server, a mutex around the map is usually the clearest design.

For the lock lab:

- `Acquire` tries to change the lock value from free to the client’s ID,
- `Release` changes it back,
- `ErrMaybe` requires checking the current state again,
- RPC failure does not imply the operation was not executed.

### Raft

Raft requires the most discipline:

- protect shared Raft state with `rf.mu`,
- do not hold `rf.mu` while sending RPCs,
- do not hold `rf.mu` while sending on `applyCh`,
- check term and role after every RPC reply,
- make every long-running goroutine exit when killed,
- use the race detector frequently,
- treat old replies and delayed events as normal, not exceptional.

---

## 10. Practical Debugging Checklist

When a test hangs:

1. Print goroutine stacks.
2. Look for goroutines stuck in channel send, channel receive, mutex lock, or select.
3. Search for blocking operations while holding locks.
4. Check whether every goroutine has an exit path.
5. Check whether stale async results are being applied to current state.

When a test behaves randomly:

1. Run `go test -race`.
2. Protect all shared state.
3. Re-check state after RPC replies.
4. Avoid assuming timing guarantees from `time.Sleep`.

---

## 11. The Core Questions to Ask

For every concurrent design, ask:

1. Who owns this state?
2. If state is shared, what protects it?
3. Who sends on this channel?
4. Who receives from this channel?
5. Can this send or receive block forever?
6. When does this goroutine exit?
7. Am I holding a lock while doing something that may block?
8. If an RPC reply arrives late, is it still valid?

If you keep asking these questions, the later 6.5840 labs become much more manageable.

---

## One-Sentence Summary

> Go concurrency in 6.5840 is about clear ownership, careful synchronization, bounded goroutine lifetimes, and never trusting that asynchronous events arrive in the order you hoped.

