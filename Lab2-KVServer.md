# Preparation

- **How the server works**
	Each client interacts with the key/value server using a _Clerk_, a set of library routines which sends RPCs to the server. Clients can send two different RPCs to the server: *Put(key, value, version) and Get(key)*. The server maintains an *in-memory* map that records for each key a (value, version) tuple. Keys and values are strings. The version number records the number of times the key has been written. *Put(key, value, version)* installs or replaces the value for a particular key in the map _only if_ the Put's version number matches the server's version number for the key. If the version numbers match, the server also *increments* the version number of the key. If the version numbers don't match, the server should return *rpc.ErrVersion*. A client can create a new key by invoking Put with version number 0 (and the resulting version stored by the server will be 1). If the version number of the Put is larger than 0 and the key doesn't exist, the server should return *rpc.ErrNoKey*.
	*Get(key)* fetches the current value for the key and its associated version. If the key doesn't exist at the server, the server should return rpc.ErrNoKey.
	Maintaining a version number for each key will be useful for implementing locks using Put and ensuring at-most-once semantics for Put's when the network is unreliable and the client retransmits.
- **Tasks**
	1. *Key/value server with reliable network ([easy](https://pdos.csail.mit.edu/6.824/labs/guidance.html))*:
	Your first task is to implement a solution that works when there are no dropped messages. You'll need to add RPC-sending code to the Clerk Put/Get methods in client.go, and implement Put and Get RPC handlers in server.go.
	2. *Implementing a lock using key/value clerk ([moderate](https://pdos.csail.mit.edu/6.824/labs/guidance.html))*
	You will need to modify src/kvsrv1/lock/lock.go. Your *Acquire* and *Release* should store each lock's state in your key/value server, by calling lk.ck.Put() and lk.ck.Get().
	If a client crashes while holding a lock, the lock will never be released. In a design more sophisticated than this lab, the client would attach a [lease](https://en.wikipedia.org/wiki/Lease_\(computer_science\)#:~:text=Leases%20are%20commonly%20used%20in,to%20rely%20on%20the%20resource.) to a lock. When the lease expires, the lock server would release the lock on behalf of the client. In this lab clients don't crash and you can ignore this problem.
	3. *Key/value server with dropped messages ([moderate](https://pdos.csail.mit.edu/6.824/labs/guidance.html))*
	Now you should modify your kvsrv1/client.go to continue in the face of dropped RPC requests and replies. A return value of true from the client's ck.clnt.Call() indicates that the client received an RPC reply from the server; a return value of false indicates that it did not receive a reply (more precisely, Call() waits for a reply message for a timeout interval, and returns false if no reply arrives within that time). Your Clerk should keep re-sending an RPC until it receives a reply. Keep in mind the discussion of rpc.ErrMaybe above. Your solution shouldn't require any changes to the server.
	4. *Implementing a lock using key/value clerk and unreliable network ([easy](https://pdos.csail.mit.edu/6.824/labs/guidance.html))*
# Working

## K/V server
I started in kvsrv1/client.go, finish the Get() and Put() function
```go
// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		// Construct args and reply
		args := rpc.GetArgs{key}
		reply := rpc.GetReply{}
		// Do rpc
		slog.Debug("Client call KVserver.Get", "args", args)
		if ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply); ok {
			switch reply.Err {
			case rpc.OK:
				slog.Debug("Client call KVServer.Get successfully", "value", reply.Value, "version", reply.Version)
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				slog.Debug("Client call KVServer.Get successfully with rpc.ErrNoKey")
				return "", 0, reply.Err
			default:
				slog.Debug("Client call KVServer.Get successfully and do again", "error", reply.Err)
				time.Sleep(time.Millisecond * 100)
				continue
			}
		} else {
			// Rpc not succeed
			slog.Debug("Client call KVServer.Get fail, do again")
			time.Sleep(time.Millisecond * 100)
			continue
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// If the client is first do the rpc
	firstCall := true
	for {
		// Construct args and reply
		args := rpc.PutArgs{key, value, version}
		reply := rpc.PutReply{}
		// Do rpc
		slog.Debug("Client call KVServer.Put", "args", args)
		if ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply); ok {
			if !firstCall && reply.Err == rpc.ErrVersion {
				// A resend call with ErrVersion
				slog.Debug("Client recall KVServer.Put successful with ErrVersion, return ErrMaybe")
				return rpc.ErrMaybe
			} else {
				slog.Debug("Client recall KVServer.Put successful", "reply.Err", reply.Err)
				return reply.Err
			}
		} else if firstCall {
			// Do call later
			slog.Debug("Client first call KVServer.Put fail, do again")
			time.Sleep(time.Millisecond * 100)
			firstCall = false
			continue
		} else {
			slog.Debug("Client recall KVServer.Put fail, return ErrMaybe")
			// Two call fail
			return rpc.ErrMaybe
		}
	}
}
```
Then I do work in kvsrv1/server.go
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
	// Your code here.
	kv.kvv = make(map[string]VV)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	slog.Debug("Server do Get", "args", args)
	// Parse args
	key := args.Key
	if vv, ok := kv.kvv[key]; ok {
		// Key exist
		*reply = rpc.GetReply{vv.value, vv.version, rpc.OK}
	} else {
		// Key not exist
		*reply = rpc.GetReply{"", 0, rpc.ErrNoKey}
	}
	slog.Debug("Server Put reply with", "reply", reply)
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	slog.Debug("Server do Put", "args", args)
	// Parse args
	key := args.Key
	avv := VV{args.Value, args.Version}
	if vv, ok := kv.kvv[key]; ok {
		if vv.version == avv.version {
			// Find the key with right version, put value with version + 1
			kv.kvv[key] = VV{avv.value, avv.version + 1}
			*reply = rpc.PutReply{rpc.OK}
		} else {
			// Version wrong
			*reply = rpc.PutReply{rpc.ErrVersion}
		}
	} else if avv.version == 0 {
		kv.kvv[key] = VV{avv.value, avv.version + 1}
		*reply = rpc.PutReply{rpc.OK}
	} else {
		*reply = rpc.PutReply{rpc.ErrNoKey}
	}
	slog.Debug("Server Put reply with", "reply", reply)
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
```
The logic is very easy, no need to explain.
Here's the all-pass picture:
![[pictures/lab2-task1-all-pass.png]]
![[pictures/lab2-task3-all-pass.png]]

## Distribute lock
This task is about using the k/v server in task1 to realize a distribute lock. The hardest part is to take all cases into consideration. The code is as follows:
```go
type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname}
	return lk
}

func (lk *Lock) Acquire() {
	// Give the current client an id
	clientId := kvtest.RandValue(8)
	for {
		// Client try to acquire the lock
		switch cid, version, err := lk.ck.Get(lk.lockname); err {
		case rpc.OK:
			if cid == "" {
				// No one had the lock, try to acquire
				switch err := lk.ck.Put(lk.lockname, clientId, version); err {
				case rpc.ErrVersion:
					// Some client acquire the lock before this, do it later
					time.Sleep(time.Millisecond * 100)
					continue
				case rpc.ErrMaybe:
					// Don't know where the lock is, check it
					continue
				case rpc.OK:
					return
				}
			} else if cid == clientId {
				// The lock owner is this client (when rpc.ErrMaybe may get in the case)
				return
			} else {
				// Some client aready had the lock, do it later
				time.Sleep(time.Millisecond * 100)
				continue
			}
		case rpc.ErrNoKey:
			// Lock state not stored, put it
			lk.ck.Put(lk.lockname, clientId, 0)
		}
	}
}

func (lk *Lock) Release() {
	for {
		// Get the current lock state
		switch cid, version, err := lk.ck.Get(lk.lockname); err {
		case rpc.OK:
			// Aready release the lock
			if cid == "" {
				return
			}
			// Get lock state successfully, update the state
			switch err := lk.ck.Put(lk.lockname, "", version); err {
			case rpc.ErrMaybe:
				continue
			case rpc.OK:
				return
			case rpc.ErrVersion:
				log.Fatalln("Unexpected error")
			}
		case rpc.ErrNoKey:
			log.Fatalln("Should acquire a lock before release")
		}
	}
}
```
And the all-pass picture:
![[pictures/lab2-task2-all-pass.png]]![[pictures/lab2-task4-all-pass.png]]