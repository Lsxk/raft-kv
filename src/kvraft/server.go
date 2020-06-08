package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"time"
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

	killCh chan bool
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
	op := beNotified(ch)
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
	originOp := Op{args.Op, args.Key, args.Value, args.Cid, args.SeqNum}
	reply.WrongLeader = true
	index, _, isLeader := kv.rf.Start(originOp)
	DPrintf("args %v, %v is Leader %v", args, kv.rf.GetId(), isLeader)
	if !isLeader {
		return
	}
	ch := kv.putIfAbsent(index)
	op := beNotified(ch)
	if equalOp(originOp, op) {
		reply.WrongLeader = false
	}
}

func beNotified(ch chan Op) Op {
	select {
	case op := <-ch:
		return op
	case <-time.After(time.Second):
		return Op{}
	}
}

func equalOp(op Op, op2 Op) bool {
	return op.Key == op2.Key && op.Value == op2.Value && op.Optype == op2.Optype && op.SeqNum == op2.SeqNum && op.Cid == op2.Cid
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
	kv.killCh = make(chan bool)

	go func() {
		for {
			select {
			case <-kv.killCh:
				return
			default:
			}
			applyMsg := <-kv.applyCh
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			maxSeq, found := kv.cid2Seq[op.Cid]
			// 幂等性实现
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
