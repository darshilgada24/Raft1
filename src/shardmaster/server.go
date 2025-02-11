package shardmaster

import (
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// Your data here.
	configs []Config // indexed by config num
	ClientToResponseMapping   map[int64]chan Op
	duplicates	map[int64]int
}

type Op struct {
	Servers map[int][]string //copying these as it is from common.go
	GIDs []int
	Shard int
	GID   int
	Num int

	TypeOfCommand string
	RequestNo int // To check for stale data if index from ApplyCh is not same
	ClientId int64

	Config Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	op:= Op{RequestNo:args.RequestNo,ClientId:args.ClientId,Servers:args.Servers,TypeOfCommand:"Join"}
	_, _, isLeader := sm.rf.Start(op)

	if isLeader == false {
		//fmt.Println("Wrong leader for Get")
		reply.WrongLeader = true
	} else {
		sm.mu.Lock()
		_,clientIdPresent := sm.ClientToResponseMapping[args.ClientId]

		if clientIdPresent == false {
			//fmt.Println("Client ID not present GET")
			ch:= make(chan Op,1)
			sm.ClientToResponseMapping[args.ClientId] = ch
		}
		ch := sm.ClientToResponseMapping[args.ClientId] //To remove race
		sm.mu.Unlock()
		select {
		case  response :=<- ch:
			sm.mu.Lock()

			if response.RequestNo == args.RequestNo && response.ClientId == args.ClientId && response.TypeOfCommand == "Join"{ //Only consider requests which have same client ID and request No
				// otherwise incorrect result is returned
				reply.WrongLeader = false
				reply.Err = OK
			}
			sm.mu.Unlock()
		case <-time.After(1 * time.Second):
			reply.Err = TimedOut
		}
	}

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op:= Op{RequestNo:args.RequestNo,ClientId:args.ClientId,GIDs:args.GIDs,TypeOfCommand:"Leave"}
	_, _, isLeader := sm.rf.Start(op)

	if isLeader == false {
		//fmt.Println("Wrong leader for Get")
		reply.WrongLeader = true
	} else {
		sm.mu.Lock()
		_,clientIdPresent := sm.ClientToResponseMapping[args.ClientId]

		if clientIdPresent == false {
			//fmt.Println("Client ID not present GET")
			ch:= make(chan Op,1)
			sm.ClientToResponseMapping[args.ClientId] = ch
		}
		ch := sm.ClientToResponseMapping[args.ClientId] //To remove race
		sm.mu.Unlock()
		select {
		case  response :=<- ch:
			sm.mu.Lock()

			if response.RequestNo == args.RequestNo && response.ClientId == args.ClientId && response.TypeOfCommand == "Leave"{ //Only consider requests which have same client ID and request No
				// otherwise incorrect result is returned
				reply.WrongLeader = false
				reply.Err = OK
			}
			sm.mu.Unlock()
		case <-time.After(1 * time.Second):
			reply.Err = TimedOut
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	//arg := MoveArgs{ args.Shard,args.GID, args.ClientId, args.RequestNo}
	op:= Op{RequestNo:args.RequestNo,ClientId:args.ClientId,GID:args.GID,Shard:args.Shard,TypeOfCommand:"Move"}
	_, _, isLeader := sm.rf.Start(op)

	if isLeader == false {
		//fmt.Println("Wrong leader for Get")
		reply.WrongLeader = true
	} else {
		sm.mu.Lock()
		_,clientIdPresent := sm.ClientToResponseMapping[args.ClientId]

		if clientIdPresent == false {
			//fmt.Println("Client ID not present GET")
			ch:= make(chan Op,1)
			sm.ClientToResponseMapping[args.ClientId] = ch
		}
		ch := sm.ClientToResponseMapping[args.ClientId] //To remove race
		sm.mu.Unlock()
		select {
		case  response :=<- ch:
			sm.mu.Lock()

			if response.RequestNo == args.RequestNo && response.ClientId == args.ClientId && response.TypeOfCommand == "Move"{ //Only consider requests which have same client ID and request No
				// otherwise incorrect result is returned
				reply.WrongLeader = false
				reply.Err = OK
			}
			sm.mu.Unlock()
		case <-time.After(1 * time.Second):
			reply.Err = TimedOut
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {

	op:= Op{RequestNo:args.RequestNo,ClientId:args.ClientId,Num:args.Num,TypeOfCommand:"Query"}
	_,_,isLeader := sm.rf.Start(op)

	if isLeader == false {
		reply.WrongLeader = true
		reply.Config = Config{}
	}else {
		sm.mu.Lock()
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.WrongLeader, reply.Config = false, sm.configs[args.Num]
			sm.mu.Unlock()
			return
		}
		sm.mu.Unlock()

		sm.mu.Lock()
		_,clientIdPresent := sm.ClientToResponseMapping[args.ClientId]

		if clientIdPresent == false {
			//fmt.Println("Client ID not present GET")
			ch:= make(chan Op,1)
			sm.ClientToResponseMapping[args.ClientId] = ch
		}
		ch := sm.ClientToResponseMapping[args.ClientId] //To remove race
		sm.mu.Unlock()
		select {
		case response :=<- ch:
			sm.mu.Lock()

			if response.RequestNo == args.RequestNo && response.ClientId == args.ClientId && response.TypeOfCommand == "Query"{ //Only consider requests which have same client ID and request No
				// otherwise incorrect result is returned
				reply.WrongLeader = false
				//reply.Err = OK
				reply.Config = response.Config
			} else {
				reply.WrongLeader = true
				reply.Config = Config{}
			}
			sm.mu.Unlock()
		case <-time.After(1 * time.Second):
			reply.Err = TimedOut
			reply.WrongLeader = true
			reply.Config = Config{}
		}

	}
}

func (sm *ShardMaster) Kill() {

}
// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg,1000)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	// Your code here.
	sm.ClientToResponseMapping = make(map[int64]chan Op)
	sm.duplicates = make(map[int64]int)
	go func() {
		for {
			select {
			case applyMsg := <-sm.applyCh:
				if !applyMsg.CommandValid {continue}
				op := applyMsg.Command.(Op)
				sm.mu.Lock()
					 if op.TypeOfCommand == "Join" {
						//create a new configuration because any such change will lead to new configuration
						oldConfig := sm.configs[len(sm.configs)-1]
						newConfig := Config{}
						newConfig.Num = oldConfig.Num + 1
						newConfig.Shards = oldConfig.Shards
						newConfig.Groups = make(map[int][]string)

						//shift old replicas to newconfig
						for groupID, replicas := range sm.configs[len(sm.configs) -1].Groups {
							newConfig.Groups[groupID] = replicas
						}

						for newGroupID, newReplicas := range op.Servers {
							//add new replicas
							newConfig.Groups[newGroupID] = newReplicas

							//Balancing the configuration
							//calculate which shards are assigned to which groups so as to get which group is loaded or free
							shardsAssignment := make(map[int][]int)
							for groupID :=  range newConfig.Groups {
								shardsAssignment[groupID] = [] int{} //referred https://stackoverflow.com/questions/3387273/how-to-implement-resizable-arrays-in-go/6335721
							}
							for index, shardNum := range newConfig.Shards {
								_, alreadyPresent := shardsAssignment[shardNum]

								if alreadyPresent == false {
									shardsAssignment[shardNum] = []int{}
								}
								shardsAssignment[shardNum] = append(shardsAssignment[shardNum], index)
							}

							//ex 2 groups and all shards assigned to group 1 i.e [1,1,1,1,1,1,1,1,1,1] then it should become like [1,1,1,1,1,2,2,2,2,2]
							//forget about least transfers for now
							//Max number of shards to a particular group if there are  2 groups would be 5
							minimumShardsAssignedToAGroup := NShards / len(newConfig.Groups)

							//shifting from group Zero
							shardsToNewGroupIDFromIndexZero := make([]int,NShards)
							for groupID := range shardsAssignment {

								if groupID == 0 {
									shardsToNewGroupIDFromIndexZero = shardsAssignment[0]
									delete(shardsAssignment,0)
									shardsAssignment[newGroupID] = shardsToNewGroupIDFromIndexZero
									break
								}
							}

							//putting extra shards in new Group
							for groupID := range newConfig.Groups {
								if groupID != newGroupID {
									if len(shardsAssignment[groupID]) > minimumShardsAssignedToAGroup {
										_, alreadyPresent := shardsAssignment[newGroupID]

										if alreadyPresent == false {
											shardsAssignment[newGroupID] = []int{} //referred https://stackoverflow.com/questions/3387273/how-to-implement-resizable-arrays-in-go/6335721
										}

										shardsAssignment[newGroupID] = append(shardsAssignment[newGroupID], shardsAssignment[groupID][minimumShardsAssignedToAGroup:]...)
										shardsAssignment[groupID] = shardsAssignment[groupID][:minimumShardsAssignedToAGroup]
									}
								}
							}
							//fmt.Println("After putting all in newGroupID")
							//fmt.Println(shardsAssignment)

							//shifting extra shards from new Group one by one to other groups to balance
							for groupID := range newConfig.Groups {
								if len(shardsAssignment[newGroupID]) > minimumShardsAssignedToAGroup + 1{
									if groupID != newGroupID {
										shardsAssignment[groupID] = append(shardsAssignment[groupID], shardsAssignment[newGroupID][0])
										shardsAssignment[newGroupID] = shardsAssignment[newGroupID][1:]
									}
								}
							}
							//fmt.Println("Printing shardsAssignment")
							//fmt.Println(shardsAssignment)

							//assigning shards to new configuration
							for groupID := range shardsAssignment {
								indexes := []int{}
								indexes = shardsAssignment[groupID]
								for _,value := range indexes{
									newConfig.Shards[value] = groupID
								}
							}
							//fmt.Println("New configuration")
							//fmt.Println(newConfig)
						}
						sm.configs = append(sm.configs,newConfig)
					} else if op.TypeOfCommand == "Leave"{
						//create a new configuration because any such change will lead to new configuration
						oldConfig := sm.configs[len(sm.configs)-1]
						newConfig := Config{}
						newConfig.Num = oldConfig.Num + 1
						newConfig.Shards = oldConfig.Shards
						newConfig.Groups = make(map[int][]string)

						//shift old replicas to newconfig
						for groupID, replicas := range sm.configs[len(sm.configs) -1].Groups {
							newConfig.Groups[groupID] = replicas
						}

						for _,groupID := range op.GIDs {
							//fmt.Println("current Configuration")
							//fmt.Println(newConfig)
							//fmt.Printf("Group ID to delete: %d",groupID)
							//fmt.Println()
							//delete replicas of the groupID to be deleted
							delete(newConfig.Groups,groupID)
							//sm.rebalance(&newConfig,op.TypeOfCommand,gid)
							//Balancing the configuration
							//calculate which shards are assigned to which groups so as to get which group is loaded or free
							shardsAssignment := make(map[int][]int)
							for groupID :=  range newConfig.Groups {
								shardsAssignment[groupID] = [] int{} //referred https://stackoverflow.com/questions/3387273/how-to-implement-resizable-arrays-in-go/6335721
							}

							//check the shards assigned to groupID which is going to be deleted
							for index, shardNum := range newConfig.Shards {
								_, alreadyPresent := shardsAssignment[shardNum]

								if alreadyPresent == false {
									shardsAssignment[shardNum] = []int{}
								}
								shardsAssignment[shardNum] = append(shardsAssignment[shardNum], index)
							}
							//fmt.Println("Printing shardsAssignment")
							//fmt.Println(shardsAssignment)
							//
							//fmt.Println("New Configuration")
							//fmt.Println(newConfig)

							_,shardAssignedToDeletedGroupIDPresent := shardsAssignment[groupID]
							if shardAssignedToDeletedGroupIDPresent == true{
								//check if no groups are there
								if len(newConfig.Groups) == 0 {
									for index := range newConfig.Shards{
											newConfig.Shards[index] = 0
									}
								}else {
									//get the shards assigned to groupID which is going to be deleted
									//reassign these shards to other groups
									shardsAssigned := shardsAssignment[groupID]
									//fmt.Println("Shards to be reassigned")
									//fmt.Println(shardsAssigned)


									minimumShardsToBeAssigned := NShards / len(newConfig.Groups)
									if len (newConfig.Groups) > 10 {
										minimumShardsToBeAssigned = 1
									}

									//fmt.Println("minimumShardsToBeAssigned")
									//fmt.Println(minimumShardsToBeAssigned)

									for groupID := range newConfig.Groups {
										canAssign := minimumShardsToBeAssigned - len(shardsAssignment[groupID])
										//fmt.Printf("canAssign: %d",canAssign)
										//fmt.Println()
										for i := 0 ; i < canAssign && len(shardsAssigned) > 0;i++ {
											//fmt.Println(shardsAssigned[0])
											shardsAssignment[groupID] = append(shardsAssignment[groupID], shardsAssigned[0])
											shardsAssigned = shardsAssigned[1:]
										}
									}
								}
								//fmt.Println("After Rebalance")
								//fmt.Println(shardsAssignment)

								//delete shardsAssignment group
								delete(shardsAssignment,groupID)

								for groupID := range shardsAssignment {
									indexes := []int{}
									indexes = shardsAssignment[groupID]
									for _,value := range indexes{
										newConfig.Shards[value] = groupID
									}
								}
							}
						}
						sm.configs = append(sm.configs,newConfig)
					} else if op.TypeOfCommand == "Move" {
						//create a new configuration because any such change will lead to new configuration
						oldConfig := sm.configs[len(sm.configs)-1]
						newConfig := Config{}
						newConfig.Num = oldConfig.Num + 1
						newConfig.Shards = oldConfig.Shards
						newConfig.Groups = make(map[int][]string)

						//shift old replicas to newconfig
						for groupID, replicas := range sm.configs[len(sm.configs) -1].Groups {
							newConfig.Groups[groupID] = replicas
						}

						//check whether groupId is present or not
						_, GroupIDPresent := newConfig.Groups[op.GID]
						if GroupIDPresent == true{
						newConfig.Shards[op.Shard] = op.GID
						}

						sm.configs = append(sm.configs,newConfig)
					} else if op.TypeOfCommand == "Query" {

						//If the number is -1 or bigger than the biggest known configuration number, the shardmaster should reply with the latest configuration
						if op.Num == -1 || op.Num >= sm.configs[len(sm.configs)-1].Num{
							op.Config = sm.configs[len(sm.configs)-1]
						}else {
							op.Config = sm.configs[op.Num]
						}
					}
				sm.mu.Unlock()

				sm.mu.Lock()
				select {
				case sm.ClientToResponseMapping[op.ClientId] <- op:
					sm.mu.Unlock()
				default:
					sm.mu.Unlock()// if you remove this it goes into deadlock
				}
			}
		}
	}()
	return sm
}