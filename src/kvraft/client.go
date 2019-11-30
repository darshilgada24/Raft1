package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LastLeader int // To store who was the last leader for this clerk
	ClientId int64 // To identify each clerk
	RequestNo int // To keep track number of get and put in the log
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
	ck.RequestNo = 0
	ck.LastLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.RequestNo++
	//fmt.Printf("Inside Get for clerk %d",ck.ClientId)
	//fmt.Println()
	i:= ck.LastLeader
	//fmt.Printf(" Get Request: Key %s, client Id %d",key,ck.ClientId)
	//fmt.Println()
	for {
		args := GetArgs{key, ck.ClientId, ck.RequestNo}
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		if ok && reply.WrongLeader == false && reply.Err == OK{
			//fmt.Printf("Value received for get %s" ,reply.Value)
			//fmt.Println()
			ck.LastLeader = i
			return reply.Value
		} else if ok && reply.WrongLeader == false && reply.Err == ErrNoKey{
			//fmt.Println("key not present for get ")
			ck.LastLeader = i
			return ""
		} else {
			i = (i + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}


// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//fmt.Printf("Inside Put Append for clerk %d",ck.ClientId)
	//fmt.Println()
	ck.RequestNo++
	i:= ck.LastLeader
	//fmt.Printf(" PutAppend Request: Key %s, Value %s, client Id %d",key,value,ck.ClientId)
	//fmt.Println()
	for {
		args := PutAppendArgs{key, value,op,ck.ClientId,ck.RequestNo}
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.WrongLeader == false && reply.Err == OK{
			ck.LastLeader = i
			return
		} else {
			i = (i + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	//fmt.Println("Put Called")
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	//fmt.Println("Append Called")
	ck.PutAppend(key, value, "Append")
}