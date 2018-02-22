package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	//"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

/*
	Op结构体为具体存储到log中的内容，包括了操作类型(get、put、append)，
	键值对(Key/Value)，客户端Id和该客户端的操作Id(2者组合标示1个操作Id)
*/
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method   string
	Key      string
	Value    string
	ClientId int64
	ReqId    int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister  *raft.Persister
	data       map[string]string //data表示存储key/value
	op_count   map[int64]int64   //op_count用于记录每个节点最后1个已经执行操作的Id
	pendingOps map[int]chan Op   //pendingOps用于记录正在执行的操作
}

func (kv *RaftKV) AppendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.pendingOps[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.pendingOps[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		return op == entry
	case <-time.After(1000 * time.Millisecond):
		//log.Printf("timeout\n")
		return false
	}
}

func (kv *RaftKV) CheckDup(id int64, reqid int64) bool {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	value, ok := kv.op_count[id]
	if ok && value >= reqid {
		return true
	}
	return false

}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//entry := Op{Method: "Get", Key: args.Key, ClientId: args.ClientId, ReqId: args.ReqId}
	var entry Op
	entry.Method = "Get"
	entry.Key = args.Key
	entry.ClientId = args.ClientId
	entry.ReqId = args.ReqId

	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.data[args.Key]
		kv.op_count[args.ClientId] = args.ReqId
		//log.Printf("%d get:%v value:%s\n",kv.me,entry,reply.Value)
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Apply(args Op) {
	switch args.Method {
	case "Put":
		kv.data[args.Key] = args.Value
	case "Append":
		kv.data[args.Key] += args.Value
	}
	kv.op_count[args.ClientId] = args.ReqId
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//entry := Op{Method: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, ReqId: args.ReqId}
	var entry Op
	entry.Method = args.Op
	entry.Key = args.Key
	entry.Value = args.Value
	entry.ClientId = args.ClientId
	entry.ReqId = args.ReqId

	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func (kv *RaftKV) sendResult(index int, op Op) {
	ch, ok := kv.pendingOps[index]
	if ok {
		select {
		case <-kv.pendingOps[index]:
		default:
		}
		ch <- op
	} else {
		kv.pendingOps[index] = make(chan Op, 1)
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	// yyf wrong

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.op_count = make(map[int64]int64)
	kv.pendingOps = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {
			msg := <-kv.applyCh
			op := msg.Command.(Op)
			kv.mu.Lock()
			if !kv.CheckDup(op.ClientId, op.ReqId) {
				kv.Apply(op)
			}
			kv.sendResult(msg.Index, op)

			kv.mu.Unlock()
		}
	}()

	return kv
}
