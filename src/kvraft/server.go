package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	DataStore map[string] string //Key Value Data store
	ClientToResponseMapping map[int64] chan Response //Client to response Mapping
	duplicates map[int64] int // Client to RequestNo Map

}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	TypeOfCommand string
	RequestNo int // To check for stale data if index from ApplyCh is not same
	ClientId int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//op:= Op{Key:args.Key,TypeOfCommand:"Get",ClientId:args.ClientId,RequestNo:args.RequestNo}
	//fmt.Println("Inside Get Procedure call")
	_, _, isLeader := kv.rf.Start(Op{args.Key, "", "Get", args.RequestNo, args.ClientId})

	if isLeader == false {
		//fmt.Println("Wrong Leader for Get")
		reply.WrongLeader = true
	} else {
			kv.mu.Lock()
		    _,clientIdPresent := kv.ClientToResponseMapping[args.ClientId]

		    if clientIdPresent == false {
				//fmt.Println("Client ID not present for get")
		    	ch:= make(chan Response,1)
		    	kv.ClientToResponseMapping[args.ClientId] = ch
			}
			ch := kv.ClientToResponseMapping[args.ClientId] //To remove race
		    kv.mu.Unlock()
			select {
			case  response :=<- ch:
				 	kv.mu.Lock()
				 	if response.RequestNo == args.RequestNo && response.ClientId == args.ClientId && args.Key == response.Key{ //Only consider requests which have same client ID and request No
				 	// otherwise incorrect result is returned
				 		value, keyPresent := kv.DataStore[response.Key]
					 	if keyPresent == false {
							//fmt.Println("Key not present")
						 	reply.WrongLeader = false
						 	reply.Err = ErrNoKey
					 	} else {
						 	//fmt.Println("Key Present for Get")
						 	reply.WrongLeader = false
						 	reply.Value = value
						 	reply.Err = OK
					 	}
				 	}
				 	kv.mu.Unlock()
			case <-time.After(1 * time.Second):
				reply.Err = TimedOut
			}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Println("Put Append Procedure")
	_, _, isLeader := kv.rf.Start(Op{args.Key, args.Value, args.Op, args.RequestNo, args.ClientId})
	if isLeader == false {
		//fmt.Println("Wrong leader for PutAppend")
		reply.WrongLeader = true
	}else{
		kv.mu.Lock()
		//fmt.Println("leader for PutAppend")
		_, clientIDPresent := kv.ClientToResponseMapping[args.ClientId]

		if clientIDPresent == false {
			//fmt.Println("Client ID not present for PutAppend")
			ch := make(chan Response, 1)
			kv.ClientToResponseMapping[args.ClientId] = ch
		}

		ch := kv.ClientToResponseMapping[args.ClientId]
		kv.mu.Unlock()

		select {
		case  response:= <- ch:
			kv.mu.Lock()
			if response.RequestNo == args.RequestNo && args.ClientId == response.ClientId && response.Key == args.Key{
				reply.WrongLeader = false
				reply.Err = OK
				//fmt.Println("No Duplicate")
			} else {
				//fmt.Println("Duplicate")
				reply.WrongLeader = false
				reply.Err = Duplicate
			}
			kv.mu.Unlock()
		case <- time.After(1 * time.Second): // Unreliable fails if we dont have this
			reply.Err = TimedOut
		}
	}

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

type Response struct {
	value string
	keyPresent bool
	isDuplicate bool
	RequestNo int
	ClientId int64
	Command string
	Key string
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
	kv.DataStore = make(map[string]string)
	kv.ClientToResponseMapping = make(map[int64] chan Response)
	kv.duplicates = make(map[int64] int)

	// You may need initialization code here.

	go func() {
		for{
			msg := <- kv.applyCh
			//fmt.Println("Got applyCH")
			op := msg.Command.(Op)
			response := Response{value:"",RequestNo:op.RequestNo,ClientId:op.ClientId,Key:op.Key}
			//For Get directly send the response
			if op.TypeOfCommand!="Get" {
				response.Command = op.TypeOfCommand
				kv.mu.Lock()
				requstNoStored, duplicatePresent := kv.duplicates[op.ClientId]
				kv.mu.Unlock()
				//check whether duplicate requestNo is there is or not if is there check the requestNo, it needs to be older then the requestNo
				//we got from applyCh
				if duplicatePresent == true{
					//check whether requestNo stored is older or not if it is older then its not a duplicate respone
					if requstNoStored >= op.RequestNo {
						//fmt.Println("Duplicate detected")
						response.isDuplicate = true
					} else if requstNoStored < op.RequestNo{
						kv.mu.Lock()
						response.isDuplicate = false
						if op.TypeOfCommand == "Put" {
							kv.DataStore[op.Key] = op.Value
						}else if op.TypeOfCommand == "Append" {
							kv.DataStore[op.Key] += op.Value
						}
						kv.duplicates[op.ClientId] = op.RequestNo
						kv.mu.Unlock()
					}
				} else {
					kv.mu.Lock()
					if op.TypeOfCommand == "Put" {
						kv.DataStore[op.Key] = op.Value
					}else if op.TypeOfCommand == "Append" {
							kv.DataStore[op.Key] += op.Value
						}
					kv.duplicates[op.ClientId] = op.RequestNo
					kv.mu.Unlock()
					}
			}
			kv.mu.Lock()
			//defer kv.mu.Unlock()
			select {
				case kv.ClientToResponseMapping[op.ClientId] <- response:
					kv.mu.Unlock()
				default: // if you remove this it goes into deadlock
					kv.mu.Unlock()
			}
		}
	}()

	return kv
}
