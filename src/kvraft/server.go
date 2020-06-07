package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype string
	Key    string
	Value  string

	// 幂等性
	Cid    int64
	SeqNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db    map[string]string
	chMap map[int]chan Op

	//幂等性
	cid2Seq map[int64]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	originOp := Op{
		Optype: "Get",
		Key:    args.Key,
		Value:  strconv.FormatInt(nrand(), 10),
		Cid:    0,
		SeqNum: 0,
	}
	reply.WrongLeader = true
	_, isLeader := kv.rf.GetState()

	if !isLeader {
		return
	}
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}

	ch := kv.putIfAbsent(index)
	op := <-ch

	fmt.Printf("request is %v", originOp)
	fmt.Println()

	if equalOp(op, originOp) {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.db[op.Key]
		kv.mu.Unlock()
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	originOp := Op{
		Optype: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Cid:    args.Cid,
		SeqNum: args.SeqNum,
	}
	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = true
	if !isLeader {
		return
	}
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	ch := kv.putIfAbsent(index)
	op := <-ch
	fmt.Printf("request is %v", originOp)
	fmt.Println()
	if equalOp(originOp, op) {
		reply.WrongLeader = false
		return
	}
}

func equalOp(op Op, op2 Op) bool {
	return op.Key == op2.Key && op.Value == op2.Value && op.Optype == op2.Optype
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) putIfAbsent(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[index]; !ok {
		kv.chMap[index] = make(chan Op, 1)
	}
	return kv.chMap[index]
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.cid2Seq = make(map[int64]int)

	go func() {
		for applyMsg := range kv.applyCh {
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			maxSeq, found := kv.cid2Seq[op.Cid]
			// 幂等性实现
			fmt.Printf("get Op %v", op)
			fmt.Println()
			if !found || op.SeqNum > maxSeq {
				switch op.Optype {
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				}
				kv.cid2Seq[op.Cid] = op.SeqNum
			}

			kv.mu.Unlock()
			index := applyMsg.CommandIndex

			ch := kv.putIfAbsent(index)
			ch <- op
		}
	}()

	return kv
}
