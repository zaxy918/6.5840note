# MIT 6.5840 Lab 2: Key/Value Server

> This note records my implementation path for Lab 2.  
> The lab starts from a simple versioned key/value server, then uses it to build a lock, and finally considers unreliable RPCs.

---

## 1. Preparation

### How the Server Works

Each client interacts with the key/value server through a `Clerk`. The Clerk hides RPC details from the application.

The server supports two main operations:

```go
Get(key)
Put(key, value, version)
```

The server stores an in-memory map:

```text
key -> (value, version)
```

The version number records how many times the key has been successfully written.

### `Get`

`Get(key)` returns:

```text
value, version
```

If the key does not exist, the server returns:

```go
rpc.ErrNoKey
```

### `Put`

`Put(key, value, version)` is a conditional write.

The server updates the key only if:

```text
client version == server version
```

If the versions match:

```text
server stores the new value
server increments the version
server returns OK
```

If the versions do not match:

```go
rpc.ErrVersion
```

If the key does not exist:

- `Put` with version `0` creates the key.
- `Put` with version greater than `0` returns `rpc.ErrNoKey`.

> [!important] Why versions matter
> The version number makes `Put` behave like a simple compare-and-swap operation. This is later used to build locks and to reason about ambiguous retries under unreliable RPC.

---

## 2. Lab Tasks

The lab has four main parts:

1. **Key/value server with reliable network**  
   Implement Clerk RPC calls and server-side `Get` / `Put`.

2. **Lock using key/value Clerk**  
   Implement `Acquire` and `Release` in `kvsrv1/lock/lock.go`.

3. **Key/value server with dropped messages**  
   Modify the Clerk so it retries when RPC replies are lost.

4. **Lock using key/value Clerk under unreliable network**  
   Make the lock implementation handle uncertain `Put` results.

---

## 3. K/V Client

I started in `kvsrv1/client.go` by implementing `Get()` and `Put()`.

### `Get`

```go
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
    for {
        args := rpc.GetArgs{Key: key}
        reply := rpc.GetReply{}

        ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
        if !ok {
            time.Sleep(time.Millisecond * 100)
            continue
        }

        switch reply.Err {
        case rpc.OK:
            return reply.Value, reply.Version, rpc.OK
        case rpc.ErrNoKey:
            return "", 0, rpc.ErrNoKey
        default:
            time.Sleep(time.Millisecond * 100)
        }
    }
}
```

The key idea is:

```text
If the RPC itself fails, retry.
If the server replies with a meaningful result, return it.
```

For `Get`, `ErrNoKey` is a real result, not a reason to retry forever.

### `Put`

`Put` is more subtle because the operation may have been executed even if the reply was lost.

```go
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
    firstCall := true

    for {
        args := rpc.PutArgs{Key: key, Value: value, Version: version}
        reply := rpc.PutReply{}

        ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
        if ok {
            if !firstCall && reply.Err == rpc.ErrVersion {
                return rpc.ErrMaybe
            }
            return reply.Err
        }

        if firstCall {
            firstCall = false
            time.Sleep(time.Millisecond * 100)
            continue
        }

        return rpc.ErrMaybe
    }
}
```

### Why `ErrMaybe` is necessary

Suppose the client sends:

```go
Put("x", "new", 1)
```

The server may successfully execute it and increment the version to `2`, but the reply may be lost.

The Clerk retries:

```go
Put("x", "new", 1)
```

Now the server sees version `2`, not `1`, and returns:

```go
ErrVersion
```

But the Clerk cannot tell whether:

1. the first RPC succeeded and the reply was lost, or
2. the first RPC did not execute and some other client changed the version.

So the Clerk must return:

```go
ErrMaybe
```

meaning:

```text
The operation may or may not have been performed.
```

---

## 4. K/V Server

The server keeps an in-memory map protected by a mutex.

```go
type VV struct {
    value   string
    version rpc.Tversion
}

type KVServer struct {
    mu  sync.Mutex
    kvv map[string]VV
}

func MakeKVServer() *KVServer {
    kv := &KVServer{}
    kv.kvv = make(map[string]VV)
    return kv
}
```

The mutex is necessary because different client RPCs may be handled concurrently.

### `Get` Handler

```go
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    if vv, ok := kv.kvv[args.Key]; ok {
        *reply = rpc.GetReply{
            Value:   vv.value,
            Version: vv.version,
            Err:     rpc.OK,
        }
    } else {
        *reply = rpc.GetReply{
            Value:   "",
            Version: 0,
            Err:     rpc.ErrNoKey,
        }
    }
}
```

### `Put` Handler

```go
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    key := args.Key
    value := args.Value
    version := args.Version

    if vv, ok := kv.kvv[key]; ok {
        if vv.version == version {
            kv.kvv[key] = VV{value: value, version: version + 1}
            *reply = rpc.PutReply{Err: rpc.OK}
        } else {
            *reply = rpc.PutReply{Err: rpc.ErrVersion}
        }
        return
    }

    if version == 0 {
        kv.kvv[key] = VV{value: value, version: 1}
        *reply = rpc.PutReply{Err: rpc.OK}
    } else {
        *reply = rpc.PutReply{Err: rpc.ErrNoKey}
    }
}
```

> [!important] Key idea
> Server-side `Put` is short and deterministic: check version, update if allowed, return an error otherwise. The server does not need special logic for dropped messages; uncertainty is handled in the Clerk.

### Result

![[pictures/lab2-task1-all-pass.png]]

![[pictures/lab2-task3-all-pass.png]]

---

## 5. Distributed Lock

The second part uses the key/value server to implement a distributed lock.

Each lock is represented by one key in the KV server:

```text
lockname -> clientId
```

A natural interpretation:

```text
missing key       lock has never been created
empty string      lock is free
clientId          lock is held by that client
```

The lock structure is:

```go
type Lock struct {
    ck       kvtest.IKVClerk
    lockname string
}

func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
    return &Lock{ck: ck, lockname: lockname}
}
```

---

## 6. `Acquire`

My `Acquire` gives the current lock client a random ID, then repeatedly tries to make the lock value equal to that ID.

```go
func (lk *Lock) Acquire() {
    clientId := kvtest.RandValue(8)

    for {
        cid, version, err := lk.ck.Get(lk.lockname)

        switch err {
        case rpc.OK:
            if cid == "" {
                switch err := lk.ck.Put(lk.lockname, clientId, version); err {
                case rpc.OK:
                    return
                case rpc.ErrVersion:
                    time.Sleep(time.Millisecond * 100)
                    continue
                case rpc.ErrMaybe:
                    continue
                }
            }

            if cid == clientId {
                return
            }

            time.Sleep(time.Millisecond * 100)

        case rpc.ErrNoKey:
            lk.ck.Put(lk.lockname, clientId, 0)
        }
    }
}
```

### How it works

1. `Get(lockname)` reads the current lock owner and version.
2. If the value is empty, the lock appears free.
3. The client tries:

   ```go
   Put(lockname, clientId, version)
   ```

4. This succeeds only if the version has not changed.
5. If another client acquired the lock first, the server returns `ErrVersion`.
6. If the result is uncertain (`ErrMaybe`), the loop checks the lock state again.

> [!important] Why version is enough for mutual exclusion
> Two clients may both observe that the lock is free, but only one of their conditional `Put` operations can succeed with the old version. The other will see `ErrVersion` and retry.

> [!caution] Client identity
> The client ID is what lets the lock implementation distinguish “I acquired the lock, but the reply was lost” from “someone else owns the lock.”

---

## 7. Handling `ErrMaybe` in `Acquire`

`ErrMaybe` means:

```text
The Put may have succeeded, or it may not have.
```

So `Acquire` should not simply assume failure.

The safe idea is:

```text
After ErrMaybe, Get the lock again.
If the value is my clientId, then I own the lock.
Otherwise, keep trying.
```

In this implementation, the loop naturally does that: after `ErrMaybe`, it continues and checks the current lock value again.

---

## 8. `Release`

`Release` checks the current lock state and writes the empty string to mark the lock as free.

```go
func (lk *Lock) Release() {
    for {
        cid, version, err := lk.ck.Get(lk.lockname)

        switch err {
        case rpc.OK:
            if cid == "" {
                return
            }

            switch err := lk.ck.Put(lk.lockname, "", version); err {
            case rpc.OK:
                return
            case rpc.ErrMaybe:
                continue
            case rpc.ErrVersion:
                log.Fatalln("Unexpected error")
            }

        case rpc.ErrNoKey:
            log.Fatalln("Should acquire a lock before release")
        }
    }
}
```

### How it works

1. Read the current lock value and version.
2. If the lock is already free, return.
3. Otherwise, use conditional `Put` to change the value to `""`.
4. If `Put` returns `ErrMaybe`, retry and check the state again.

> [!note]
> The lab assumes clients do not crash while holding locks. In a real system, a lock usually needs a lease so it can eventually expire if the client dies.

> [!caution]
> A more defensive implementation could store the lock owner's `clientId` inside the `Lock` object and verify that `Release` only releases a lock held by the same client. The lab's tester may not require this, but it is an important real-world design point.

### Result

![[pictures/lab2-task2-all-pass.png]]

![[pictures/lab2-task4-all-pass.png]]

---

## 9. Final Takeaways

- The server is simple: a mutex-protected map from key to `(value, version)`.
- `Put` is a conditional write based on version.
- The Clerk retries RPCs when replies are dropped.
- `ErrMaybe` exists because a lost reply does not mean the operation failed.
- A lock can be built by storing the current owner in the KV server.
- Versioned `Put` ensures only one client can acquire a free lock.
- After uncertain writes, the client should read the state again.

