package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

/*
	客户端结构体Clerk的具体内容，LeaderId用于记录上一次成功操作通信的服务节点即记住Leader节点。
	由于只有Leader节点才能接受操作请求，这里记住Leader节点避免了每次请求都要遍历服务集群节点来找到Leader节点。
	ClientId表示客户端Id，在初始化函数中使用nrand()函数来生成随机值。ReqId用于记录最后一次发出的操作Id。
*/
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId int64
	LeaderId int
	ReqId    int64
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.ReqId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	args.ClientId = ck.ClientId
	ck.mu.Lock()
	args.ReqId = ck.ReqId
	ck.ReqId++
	ck.mu.Unlock()
	for {
		// wrong 没有return value
		var reply GetReply
		ck.servers[ck.LeaderId].Call("RaftKV.Get", &args, &reply)
		if reply.Err == OK && reply.WrongLeader == false {
			DPrintf("Call success\n")
			return reply.Value
		} else {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.ClientId
	ck.mu.Lock()
	args.ReqId = ck.ReqId
	ck.ReqId++
	ck.mu.Unlock()
	for {
		/*for _,v := range ck.servers {
			var reply PutAppendReply
			ok := v.Call("RaftKV.PutAppend", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}*/
		var reply PutAppendReply
		ck.servers[ck.LeaderId].Call("RaftKV.PutAppend", &args, &reply)
		if reply.Err == OK && reply.WrongLeader == false {
			DPrintf("Call success\n")
			break
		} else {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
