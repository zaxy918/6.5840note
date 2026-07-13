# MIT 6.5840 Lab 1: MapReduce

> This note records my implementation path for Lab 1.  
> It is not a from-scratch tutorial; it is a structured review of my own design, with extra explanations for the key implementation decisions.

---

## 1. Preparation

### MapReduce Workflow

The MapReduce system in this lab has one coordinator and multiple workers. The coordinator is responsible for assigning tasks, tracking task progress, detecting failed workers, and deciding when the whole job is finished.

The overall workflow is:

1. The coordinator starts with a list of input files. Each input file corresponds to one map task.
2. A worker asks the coordinator for work through RPC.
3. If assigned a map task, the worker reads the input file, calls the user-defined `mapf`, and produces intermediate key/value pairs.
4. The worker partitions intermediate key/value pairs into `nReduce` groups using:

   ```go
   ihash(key) % nReduce
   ```

5. Each map task writes intermediate files named:

   ```text
   mr-X-Y
   ```

   where `X` is the map task ID and `Y` is the reduce task ID.

6. After all map tasks finish, the coordinator starts assigning reduce tasks.
7. A reduce worker reads all intermediate files for one reduce partition, groups values by key, calls `reducef`, and writes the final output file:

   ```text
   mr-out-Y
   ```

8. When all reduce tasks finish, workers exit and the coordinator reports completion through `Done()`.

![[pictures/mapreduce-workflow.png|820]]

> [!important] Lab constraints
> Only `mr/worker.go`, `mr/coordinator.go`, and `mr/rpc.go` should be modified. The coordinator must support concurrent RPC calls, and tasks should be considered failed if a worker does not finish within about 10 seconds.

> [!tip] Useful implementation hints
> - Start from `Worker()` in `mr/worker.go`.
> - Use Go RPC for communication between workers and the coordinator.
> - Use `encoding/json` for intermediate key/value data.
> - Use `os.CreateTemp` and `os.Rename` so output files appear atomically.
> - Use `mrapps/crash.go` to test failure recovery.

---

## 2. Overall Structure

I started from `Worker()` because `main/mrworker.go` directly calls this function. My first goal was to make the worker repeatedly ask the coordinator for a task.

```go
func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
    coordSockName = sockname
    args := WorkerArgs{Status: Wait, IsFirstCall: true}

    for {
        reply := WorkerReply{}
        ok := call("Coordinator.Assign", &args, &reply)

        if reply.Status == Wait {
            time.Sleep(time.Millisecond * 50)
            continue
        }

        if !ok {
            log.Fatalf("RPC call failure")
        }

        switch reply.Status {
        case MapTask:
            // do map work
        case ReduceTask:
            // do reduce work
        case Exit:
            os.Exit(0)
        }
    }
}
```

The worker is a loop:

```text
ask coordinator -> receive task -> execute task -> report result -> ask again
```

This structure keeps the coordinator in control of task assignment while allowing workers to be simple and stateless between RPCs.

---

## 3. RPC Types

I defined task status and RPC argument/reply types in `mr/rpc.go`.

```go
type Status int

const (
    MapTask Status = iota
    ReduceTask
    Wait
    MapDone
    ReduceDone
    Exit
)

type WorkerArgs struct {
    Status         Status
    IsFirstCall    bool
    InterFileNames []string
    WorkerId       int
}

type WorkerReply struct {
    Status   Status
    WorkerId int
    TaskId   int
    Files    []string
    NReduce  int
}
```

### Why `WorkerId` and `TaskId` are separate

`WorkerId` identifies a worker. It is assigned when the worker first contacts the coordinator.

`TaskId` identifies a map or reduce task. It is assigned when a task is created and must remain stable even if the task is retried after worker failure.

This distinction matters because output file names depend on task IDs:

```text
mr-X-Y
mr-out-Y
```

If retried tasks received new task IDs, output file naming could become inconsistent.

> [!note]
> My first design only had `TaskId`. That was enough for the normal case, but not enough for fault tolerance. Once worker failure is considered, the coordinator needs to track both the worker and the task assigned to it.

---

## 4. Coordinator Skeleton

The coordinator handles all worker RPCs through `Assign()`.

```go
func (c *Coordinator) Assign(args *WorkerArgs, reply *WorkerReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if args.IsFirstCall {
        // initialize worker state
    }

    switch args.Status {
    case Wait:
        // assign a new task if possible
    case MapDone:
        // record intermediate files
    case ReduceDone:
        // record reduce completion
    default:
        return errors.New("invalid task status")
    }

    return nil
}
```

The coordinator uses a mutex because multiple workers may call `Assign()` concurrently. All shared coordinator state must be protected.

---

## 5. Map Task Implementation

When the worker receives a `MapTask`, it reads the assigned file, calls `mapf`, and writes intermediate files.

```go
case MapTask:
    slog.Debug("Start map task", "worker_id", reply.WorkerId, "task_id", reply.TaskId)

    kvs := []KeyValue{}
    for _, filename := range files {
        content, err := os.ReadFile(filename)
        if err != nil {
            log.Fatalf("Read file fail: %v", filename)
        }
        kvs = append(kvs, mapf(filename, string(content))...)
    }

    interFileNames := writeIntermediate(kvs, reply.TaskId)
    args = WorkerArgs{
        Status:         MapDone,
        InterFileNames: interFileNames,
        WorkerId:       reply.WorkerId,
    }
```

In this lab, each map task has one input file, but I used a loop over `files` so the design can still fit a more general form.

### Writing Intermediate Files

```go
func writeIntermediate(kvs []KeyValue, taskId int) []string {
    fileToContent := make(map[string][]KeyValue)

    for _, kv := range kvs {
        reducerId := ihash(kv.Key) % nReduce
        filename := fmt.Sprintf("mr-%v-%v", taskId, reducerId)
        fileToContent[filename] = append(fileToContent[filename], kv)
    }

    interFileNames := []string{}
    for filename, kvs := range fileToContent {
        interFileNames = append(interFileNames, filename)

        fTemp, err := os.CreateTemp("", "mr-tmp-*")
        if err != nil {
            log.Fatal(err)
        }
        tempPath := fTemp.Name()

        enc := json.NewEncoder(fTemp)
        if err := enc.Encode(kvs); err != nil {
            log.Fatal(err)
        }
        fTemp.Close()

        if err := os.Rename(tempPath, fmt.Sprintf("/tmp/%v", filename)); err != nil {
            log.Fatal(err)
        }
    }

    return interFileNames
}
```

> [!important] Key idea
> I first group key/value pairs by output file name in memory, then write each file once. This avoids repeatedly opening and writing the same intermediate file.

> [!note]
> I used temporary files plus `os.Rename` so that an intermediate file becomes visible atomically only after it is fully written.

---

## 6. Coordinator State

The coordinator needs to track worker status, task progress, intermediate files, and failure recovery.

```go
type Coordinator struct {
    mu                sync.Mutex
    workerId          int
    allMapDone        bool
    allReduceDone     bool
    interFileCnt      int
    nReduce           int
    aliveWorker       int
    taskId            int
    mapperCnt         int
    reducerCnt        int
    inputFile         []string
    intermediateFiles map[int][]string
    workerList        []WorkerEntry
    failTaskFiles     []Task
}
```

The fields are somewhat verbose, but each one corresponds to one responsibility:

- `inputFile`: map tasks not yet assigned.
- `intermediateFiles`: reduce partition ID to intermediate file names.
- `mapperCnt` / `reducerCnt`: tasks currently running.
- `allMapDone` / `allReduceDone`: phase transition and shutdown.
- `workerList`: worker status for failure detection.
- `failTaskFiles`: tasks that need to be retried.

---

## 7. Assigning Map Tasks

In the `Wait` case, if map work remains, the coordinator assigns one input file to the worker.

```go
case Wait:
    switch {
    case !c.allMapDone && len(c.inputFile) > 0:
        filename := c.inputFile[0]
        c.inputFile = c.inputFile[1:]

        worker = WorkerEntry{
            status:   MapTask,
            workerId: worker.workerId,
            files:    []string{filename},
            deadLine: getExpireTime(),
            taskId:   c.taskId,
        }

        c.replyAndSetWorkerList(&worker, reply)
        c.mapperCnt++
        c.taskId++
    }
```

The coordinator removes the input file from the unassigned list and records the worker as running a map task.

The deadline is used later for fault tolerance.

---

## 8. Handling `MapDone`

When a worker finishes a map task, it sends the intermediate file names back to the coordinator.

```go
case MapDone:
    for _, filename := range args.InterFileNames {
        parts := strings.Split(filename, "-")
        reducerId, err := strconv.Atoi(parts[len(parts)-1])
        if err != nil {
            log.Fatal(err)
        }
        c.intermediateFiles[reducerId] = append(c.intermediateFiles[reducerId], filename)
    }

    c.mapperCnt--
    c.interFileCnt += len(args.InterFileNames)

    if c.mapperCnt == 0 && len(c.inputFile) == 0 && len(c.failTaskFiles) == 0 {
        c.allMapDone = true
        c.taskId = 0
    }

    worker = WorkerEntry{status: Wait, workerId: worker.workerId, deadLine: getExpireTime(), taskId: -1}
    c.replyAndSetWorkerList(&worker, reply)
```

### Why reset `taskId`

Map tasks and reduce tasks both use task IDs starting from zero. After all map tasks finish, I reset `taskId` so reduce task IDs match output files:

```text
mr-out-0
mr-out-1
...
```

---

## 9. Reduce Task Implementation

When a worker receives a reduce task, it reads all intermediate files assigned to that reduce partition, sorts key/value pairs, groups values by key, and calls `reducef`.

```go
case ReduceTask:
    interFileNames := reply.Files
    reducerId := reply.TaskId

    kvs := readFromInterFiles(interFileNames)
    sort.Sort(KVList(kvs))

    keyToAllValues := make(map[string][]string)
    for _, kv := range kvs {
        keyToAllValues[kv.Key] = append(keyToAllValues[kv.Key], kv.Value)
    }

    oFile, err := os.Create(fmt.Sprintf("mr-out-%v", reducerId))
    if err != nil {
        log.Fatalf("Create output file failed: %v", err)
    }

    for k, vs := range keyToAllValues {
        res := reducef(k, vs)
        fmt.Fprintf(oFile, "%v %v\n", k, res)
    }

    args = WorkerArgs{Status: ReduceDone, WorkerId: reply.WorkerId}
```

The helper function reads JSON-encoded intermediate files:

```go
func readFromInterFiles(interFileNames []string) (allkvs []KeyValue) {
    for _, filename := range interFileNames {
        f, err := os.Open(fmt.Sprintf("/tmp/%v", filename))
        if err != nil {
            log.Fatal(err)
        }

        var kvs []KeyValue
        dec := json.NewDecoder(f)
        if err = dec.Decode(&kvs); err != nil {
            log.Fatal(err)
        }

        allkvs = append(allkvs, kvs...)
    }
    return
}
```

> [!caution] Output ordering
> The code sorts `kvs`, but then stores grouped values in a Go map. Iterating over a Go map does not produce deterministic key order. The lab tests usually sort output before comparison, but for cleaner deterministic output, it is better to iterate directly over the sorted `kvs` and group adjacent equal keys.

> [!caution] Comparator typo to watch for
> `Less(i, j)` should compare `kv[i].Key < kv[j].Key`. If it compares `kv[i].Key < kv[i].Key`, sorting will not work.

---

## 10. Assigning Reduce Tasks

After all map tasks finish, reduce tasks can be assigned.

```go
case c.interFileCnt > 0 && c.allMapDone:
    interFileNames := c.intermediateFiles[c.taskId]
    c.interFileCnt -= len(interFileNames)

    worker = WorkerEntry{
        status:   ReduceTask,
        workerId: worker.workerId,
        files:    interFileNames,
        deadLine: getExpireTime(),
        taskId:   c.taskId,
    }

    c.replyAndSetWorkerList(&worker, reply)
    c.taskId++
    c.reducerCnt++
```

Each reduce task corresponds to one reduce partition. The coordinator sends that partition's intermediate file list to the worker.

### Handling `ReduceDone`

```go
case ReduceDone:
    c.reducerCnt--

    if c.reducerCnt == 0 && c.interFileCnt == 0 && len(c.failTaskFiles) == 0 {
        c.allReduceDone = true
    }

    worker = WorkerEntry{status: Wait, workerId: worker.workerId, deadLine: getExpireTime(), taskId: -1}
    c.replyAndSetWorkerList(&worker, reply)
```

When no reduce tasks are running, no intermediate files remain unassigned, and there are no failed tasks waiting for retry, the whole MapReduce job is done.

---

## 11. Termination

The worker exits when the coordinator replies with `Exit`.

```go
case Exit:
    os.Exit(0)
```

On the coordinator side:

```go
case c.allReduceDone:
    worker.status = Exit
    c.replyAndSetWorkerList(&worker, reply)
    c.aliveWorker--
```

The coordinator's `Done()` function is periodically called by `main/mrcoordinator.go`.

```go
func (c *Coordinator) Done() bool {
    c.mu.Lock()
    defer c.mu.Unlock()

    return c.aliveWorker == 0 && c.allReduceDone
}
```

### Worker initialization

When a worker first contacts the coordinator, it receives a `WorkerId` and is added to `workerList`.

```go
if args.IsFirstCall {
    worker.workerId = c.workerId
    worker.taskId = -1
    c.workerList = append(c.workerList, worker)
    c.workerId++
    c.aliveWorker++
}
```

This gives the coordinator a stable identity for tracking worker deadlines and assigned tasks.

---

## 12. Fault Tolerance

The main fault-tolerance idea is:

> If a worker does not complete its assigned task within about 10 seconds, treat the worker as failed and put its task back into a retry queue.

I use:

- `WorkerId` to identify workers,
- `TaskId` to identify tasks,
- `WorkerEntry` to track worker state,
- `Task` to record failed tasks.

```go
func getExpireTime() time.Time {
    return time.Now().Add(time.Second * 10)
}

type WorkerEntry struct {
    status   Status
    workerId int
    files    []string
    deadLine time.Time
    taskId   int
}

type Task struct {
    taskId int
    status Status
    files  []string
}
```

### Periodic worker checking

```go
func (c *Coordinator) checkWorkerStatus() {
    for {
        time.Sleep(time.Second)

        c.mu.Lock()
        for _, worker := range c.workerList {
            if worker.status != Wait &&
                time.Now().After(worker.deadLine) &&
                worker.status != Exit {

                c.failTaskFiles = append(c.failTaskFiles, Task{
                    taskId: worker.taskId,
                    status: worker.status,
                    files:  worker.files,
                })

                c.workerList[worker.workerId].status = Exit
                c.aliveWorker--
                c.cond.Broadcast()
            }
        }
        c.mu.Unlock()
    }
}
```

When a worker is considered dead, its task is moved into `failTaskFiles`.

### Reassigning failed tasks

```go
case len(c.failTaskFiles) > 0:
    task := c.failTaskFiles[0]
    c.failTaskFiles = c.failTaskFiles[1:]

    worker = WorkerEntry{
        status:   task.status,
        workerId: worker.workerId,
        files:    task.files,
        deadLine: getExpireTime(),
        taskId:   task.taskId,
    }

    c.replyAndSetWorkerList(&worker, reply)
```

The retried task keeps its original `taskId`. This is important because file names depend on task ID.

> [!important] Why not generate a new task ID?
> If a retried reduce task received a new ID, it would write a different `mr-out-*` file. The tests expect output files by reduce partition number, so task IDs must stay stable across retries.

---

## 13. Refinement: `sync.Cond`

My first design returned `Wait` to workers when no task was available. The worker then slept for 50 milliseconds and asked again.

That works, but it causes repeated RPC calls while workers are idle.

After watching the Lab 1 Q&A, I refined this with `sync.Cond`.

### Original behavior

```text
worker gets Wait -> sleep 50ms -> send RPC again
```

### Refined behavior

```text
worker enters RPC -> coordinator waits inside Assign()
coordinator wakes waiting RPCs when useful work appears
```

The coordinator has:

```go
type Coordinator struct {
    cond *sync.Cond
    ...
}
```

Initialized with:

```go
c.cond = sync.NewCond(&c.mu)
```

When no work is available:

```go
c.cond.Wait()
```

When something changes:

```go
c.cond.Broadcast()
```

Broadcast happens when:

- all map tasks finish,
- a failed task is added,
- all reduce tasks finish.

> [!note] Why this helps
> Instead of making idle workers repeatedly poll the coordinator, the coordinator holds the RPC until there is a useful reason for the worker to try again.

> [!caution] Cond variables need care
> `sync.Cond.Wait()` must be used while holding the associated lock. Also, every condition that can make progress possible must eventually call `Broadcast()` or `Signal()`.

---

## 14. Result

The implementation passed all Lab 1 tests.

![[pictures/lab1-all-pass.png]]

---

## 15. Final Takeaways

- Keep `WorkerId` and `TaskId` separate.
- Use stable task IDs because output file names depend on them.
- Protect all coordinator state with a mutex.
- Write intermediate files atomically with temp files and rename.
- Do not start reduce tasks until all map tasks finish.
- Treat long-running workers as failed and retry their tasks.
- Be careful with duplicate or late task completion.
- `sync.Cond` can reduce polling, but it must be used carefully.

