package server

import (
	"DongoMQ/kitex_gen/api"
	"DongoMQ/kitex_gen/api/raft_operations"
	"DongoMQ/logger"
	"DongoMQ/raft"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/server"
)

const (
	TIMEOUT        = 1000 * 1000
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWrongNum    = "ErrWrongNum"
)

type COMD struct {
	index   int
	The_num int
}

type SnapShot struct {
	Tpart        string
	Csm          map[string]int64
	Cdm          map[string]int64
	AppliedIndex int
}

type parts_raft struct {
	mu         sync.RWMutex
	srv_raft   server.Server
	Partitions map[string]*raft.Raft
	Leaders    map[string]bool

	me      int
	appench chan info // 和外部通信的通道，传递需要被Raft处理的消息
	applyCh chan raft.ApplyMsg

	maxraftstate int   //snapshot if log grows this big
	dead         int32 //set by kill()

	Add chan COMD

	// 幂等性保证
	CSM map[string]map[string]int64 // 记录每个分区、每个生产者的当前状态
	CDM map[string]map[string]int64 // 记录每个分区、每个生产者的最新命令索引

	//多raft,则需要多applyindex
	applyindexs map[string]int
}

func NewParts_Raft() *parts_raft {
	return &parts_raft{
		mu: sync.RWMutex{},
	}
}

func (p *parts_raft) Make(name string, opts []server.Option, appench chan info, me int) {
	p.appench = appench
	p.me = me
	p.applyCh = make(chan raft.ApplyMsg)
	p.Add = make(chan COMD)

	p.CDM = make(map[string]map[string]int64)
	p.CSM = make(map[string]map[string]int64)
	p.Partitions = make(map[string]*raft.Raft)
	p.applyindexs = make(map[string]int)
	p.Leaders = make(map[string]bool)

	srv_raft := raft_operations.NewServer(p, opts...)
	p.srv_raft = srv_raft

	go func() {
		err := srv_raft.Run()

		if err != nil {
			logger.DEBUG_RAFT(logger.DError, "the raft run fail %v\n", err.Error())
		}
	}()
}

func (p *parts_raft) Append(in info) (string, error) {
	str := in.topicName + in.partName
	logger.DEBUG_RAFT(logger.DLeader, "[S%d <-- pro %v] append message(%v) topic_partition(%v)\n", p.me, in.producer, in.cmdIndex)

	p.mu.Lock()
	//检查当前partition是否接收消息
	_, ok := p.Partitions[str]
	if !ok {
		ret := "partition remove"
		logger.DEBUG_RAFT(logger.DLog, "this partition(%v) is not in this broker", str)
		p.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		return ret, errors.New(ret)
	}

	_, isLeader := p.Partitions[str].GetState()
	if !isLeader {
		logger.DEBUG_RAFT(logger.DLog, "raft %d is not the leader", p.me)
		p.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		return ErrWrongLeader, nil
	}

	if p.applyindexs[str] == 0 {
		logger.DEBUG_RAFT(logger.DLog, "%d the snap not applied applyindex is %v\n", p.me, p.applyindexs[str])
		p.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		return ErrTimeout, nil
	}

	// 幂等性检查
	_, ok = p.CDM[str]
	if !ok {
		logger.DEBUG_RAFT(logger.DLog, "%d make CDM Tpart(%v)", p.me, str)
		p.CDM[str] = make(map[string]int64)
	}
	_, ok = p.CSM[str]
	if !ok {
		logger.DEBUG_RAFT(logger.DLog, "%d make CSM Tpart(%v)", p.me, str)
		p.CSM[str] = make(map[string]int64)
	}

	in1, ok1 := p.CDM[str][in.producer]
	if ok1 && in1 == in.cmdIndex {
		logger.DEBUG_RAFT(logger.DInfo, "%d p.CDM[%v][%v](%v) in.cmdindex(%v)\n", p.me, str, in.producer, p.CDM[str][in.producer], in.cmdIndex)
		p.mu.Unlock()
		return OK, nil
	} else if !ok1 {
		logger.DEBUG_RAFT(logger.DLog, "%d add CDM[%v][%v](%v)\n", p.me, str, in.producer, 0)
		p.CDM[str][in.producer] = 0
	}
	p.mu.Unlock()
	var index int
	Op := raft.Operation{
		Ser_index: int64(p.me),
		Cli_name:  in.producer,
		Cmd_index: in.cmdIndex,
		Operate:   "Append",
		Topic:     in.topicName,
		Part:      in.partName,
		Tpart:     str,
		Msg:       in.message,
		Size:      in.size,
	}

	p.mu.Lock()
	logger.DEBUG_RAFT(logger.DLog, "%d lock 285\n", p.me)
	in2, ok2 := p.CSM[str][in.producer]
	if !ok2 {
		logger.DEBUG_RAFT(logger.DLog, "%d add CSM[%v][%v](%v)\n", p.me, str, in.producer, 0)
		p.CSM[str][in.producer] = 0
	}
	p.mu.Unlock()

	logger.DEBUG_RAFT(logger.DInfo, "%d p.CSM[%v][%v](%v) in.cmdIndex(%v)\n", p.me, str, in.producer, p.CSM[str][in.producer], in.cmdIndex)
	if in2 == in.cmdIndex {
		_, isLeader = p.Partitions[str].GetState()
	} else {
		index, _, isLeader = p.Partitions[str].Start(Op, false, 0)
	}

	if !isLeader {
		return ErrWrongLeader, nil
	} else {
		p.mu.Lock()
		lastIndex, ok := p.CSM[str][in.producer]
		if !ok {
			p.CSM[str][in.producer] = 0
		}
		p.CSM[str][in.producer] = in.cmdIndex
		p.mu.Unlock()

		for {
			select {
			case out := <-p.Add:
				if index == out.index {
					return OK, nil
				} else {
					logger.DEBUG_RAFT(logger.DLog, "%d cmd index %d != out.index %d\n", p.me, index, out.index)
				}
			case <-time.After(TIMEOUT * time.Millisecond):
				_, isLeader := p.Partitions[str].GetState()
				ret := ErrTimeout
				p.mu.Lock()
				logger.DEBUG_RAFT(logger.DLog, "%d lock 332\n", p.me)
				logger.DEBUG_RAFT(logger.DLeader, "%d time out\n", p.me)
				if !isLeader {
					ret = ErrWrongLeader
					p.CSM[str][in.producer] = lastIndex
				}
				p.mu.Unlock()
				return ret, nil
			}
		}
	}
}

func (p *parts_raft) Kill(str string) {
	atomic.StoreInt32(&p.dead, 1)
	logger.DEBUG_RAFT(logger.DLog, "%d kill\n", p.me)
}

func (p *parts_raft) Killed() bool {
	z := atomic.LoadInt32(&p.dead)
	return z == 1
}

func (p *parts_raft) SendSnapShot(str string) {
	w := new(bytes.Buffer)
	e := raft.NewEncoder(w)
	S := SnapShot{
		Csm:          p.CSM[str],
		Cdm:          p.CDM[str],
		Tpart:        str,
		AppliedIndex: p.applyindexs[str],
	}
	e.Encode(S)
	logger.DEBUG_RAFT(logger.DSnap, "%d the size need to snap\n", p.me)
	data := w.Bytes()
	go p.Partitions[str].Snapshot(S.AppliedIndex, data)
}

func (p *parts_raft) CheckSnap() {
	for str, raft := range p.Partitions {
		X, num := raft.RaftSize()
		logger.DEBUG_RAFT(logger.DSnap, "%d the size is %v appliedIndex(%v) X(%v)\n", p.me, num, p.applyindexs[str], X)
		if num >= int(float64(p.maxraftstate)) {
			if p.applyindexs[str] == 0 || p.applyindexs[str] <= X {
				return
			}
			p.SendSnapShot(str)
		}
	}
}

func (p *parts_raft) StartServer() {
	logger.DEBUG_RAFT(logger.DSnap, "%d parts_raft start\n", p.me)

	go func() {
		for {
			if !p.Killed() {
				select {
				case m := <-p.applyCh:
					if m.BeLeader {
						str := m.TopicName + m.PartName
						logger.DEBUG_RAFT(logger.DLog, "%d Broker tPart(%v) become leader apply from %v to %v\n", p.me, str, p.applyindexs[str], m.CommandIndex)
						p.applyindexs[str] = m.CommandIndex
						if m.Leader == p.me {
							p.appench <- info{
								producer:  "Leader",
								topicName: m.TopicName,
								partName:  m.PartName,
							}
						}
					} else if m.CommandValid && !m.BeLeader {
						p.mu.Lock()

						O := m.Command

						_, ok := p.CDM[O.Tpart]
						if !ok {
							logger.DEBUG_RAFT(logger.DLog, "%d make CDM TPart(%v)\n", p.me, O.Tpart)
							p.CDM[O.Tpart] = make(map[string]int64)
						}
						_, ok = p.CSM[O.Tpart]
						if !ok {
							logger.DEBUG_RAFT(logger.DLog, "%d make CSM TPart(%v)\n", p.me, O.Tpart)
							p.CSM[O.Tpart] = make(map[string]int64)
						}

						logger.DEBUG_RAFT(logger.DLog, "%d CommandValid(%v) applyindex[%v](%v) commandIndex(%v) CDM[%v][%v](%v) O.cmd_index(%v) from %v\n", p.me, m.CommandValid, O.Tpart, p.applyindexs[O.Tpart], m.CommandIndex, O.Tpart, O.Cli_name, p.CDM[O.Tpart][O.Cli_name], O.Cmd_index, O.Ser_index)

						if p.applyindexs[O.Tpart]+1 == m.CommandIndex {
							if O.Cli_name == "TIMEOUT" {
								logger.DEBUG_RAFT(logger.DLog, "%d for TIMEOUT update applyindex %v to %v\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex
							} else if p.CDM[O.Tpart][O.Cli_name] < O.Cmd_index {
								logger.DEBUG_RAFT(logger.DLeader, "S%d get message update CDM[%v][%v] from %v to %v update applyindex from %v to %v\n", p.me, O.Tpart, O.Cli_name, p.CDM[O.Tpart][O.Cli_name], O.Cmd_index, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex

								p.CDM[O.Tpart][O.Cli_name] = O.Cmd_index
								if O.Operate == "Append" {
									p.appench <- info{
										producer:  O.Cli_name,
										message:   O.Msg,
										topicName: O.Topic,
										partName:  O.Part,
										size:      O.Size,
									}

									select {
									case p.Add <- COMD{index: m.CommandIndex}:

									default:

									}
								}
							} else if p.CDM[O.Tpart][O.Cli_name] == O.Cmd_index {
								logger.DEBUG_RAFT(logger.DLog2, "S%d this cmd had done,the log had two update applyindex %v to %v\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex
							} else {
								logger.DEBUG_RAFT(logger.DLog2, "S%d the topic-partition(%v) producer(%v) OIndex(%v) < CDM(%v)\n", p.me, O.Tpart, O.Cli_name, O.Cmd_index, p.CDM[O.Tpart][O.Cli_name])
								p.applyindexs[O.Tpart] = m.CommandIndex
							}
						} else if p.applyindexs[O.Tpart]+1 < m.CommandIndex {
							logger.DEBUG_RAFT(logger.DWarn, "%d the applyindex + 1(%v) < commanindex(%v)\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
						}

						if p.maxraftstate > 0 {
							p.CheckSnap()
						}

						p.mu.Unlock()
					} else { //read snapshot
						r := bytes.NewBuffer(m.Snapshot)
						d := raft.NewDecoder(r)
						logger.DEBUG_RAFT(logger.DSnap, "%d the snapshot applied", p.me)
						var S SnapShot
						p.mu.Lock()
						if d.Decode(&S) != nil {
							p.mu.Unlock()
							logger.DEBUG_RAFT(logger.DSnap, "%d labgob failed", p.me)
						} else {
							p.CDM[S.Tpart] = S.Cdm
							p.CSM[S.Tpart] = S.Csm

							logger.DEBUG_RAFT(logger.DSnap, "S%d recover by SnapShot update applyindex(%v) to %v\n", p.me, p.applyindexs[S.Tpart], S.AppliedIndex)
							p.applyindexs[S.Tpart] = S.AppliedIndex
							p.mu.Unlock()
						}
					}
				case <-time.After(TIMEOUT * time.Millisecond):
					O := raft.Operation{
						Ser_index: int64(p.me),
						Cli_name:  "TIMEOUT",
						Cmd_index: -1,
						Operate:   "TIMEOUT",
					}
					logger.DEBUG_RAFT(logger.DLog, "S%d have log time applied\n", p.me)
					p.mu.RLock()
					for str, raft := range p.Partitions {
						O.Tpart = str
						raft.Start(O, false, 0)
					}
					p.mu.RUnlock()
				}
			}
		}
	}()
}

// 检查或创建一个raft
// 添加一个需要raft同步的partition
func (p *parts_raft) AddPart_Raft(peers []*raft_operations.Client, me int, topicName, partName string) {
	//启动一个raft，即调用Make()，需要提供各节点broker的raft_clients，和该partition的通道
	str := topicName + partName
	p.mu.Lock()
	_, ok := p.Partitions[str]
	if !ok {
		per := &raft.Persister{}
		part_raft := raft.Make(peers, me, per, p.applyCh, topicName, partName)
		p.Partitions[str] = part_raft
	}
	p.mu.Unlock()
}

func (p *parts_raft) CheckPartState(topicName, partName string) bool {
	str := topicName + partName
	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.Partitions[str]
	return ok
}

func (p *parts_raft) DeletePart_raft(topicName, partName string) error {
	str := topicName + partName

	p.mu.Lock()
	defer p.mu.Unlock()
	raft, ok := p.Partitions[str]
	if !ok {
		logger.DEBUG_RAFT(logger.DError, "this topic-partition(%v) is not in this broker\n", str)
		return errors.New("this topic-partition is not in this broker")
	} else {
		raft.Kill()
		delete(p.Partitions, str)
		return nil
	}
}

func (p *parts_raft) RequestVote(ctx context.Context, args *api.RequestVoteArgs_) (r *api.RequestVoteReply, err error) {
	str := args.TopicName + args.PartName
	p.mu.RLock()
	raft_ptr, ok := p.Partitions[str]
	p.mu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DWarn, "raft(%v) is not get\n", str)
		time.Sleep(time.Second * 10)
		return
	}

	reply := raft_ptr.RequestVote(&raft.RequestVoteArgs{
		Term:         int(args.Term),
		CandidateId:  int(args.CandidateId),
		LastLogIndex: int(args.LastLogIndex),
		LastLogTerm:  int(args.LastLogTerm),
	})

	return &api.RequestVoteReply{
		Term:        int8(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (p *parts_raft) AppendEntries(ctx context.Context, args *api.AppendEntriesArgs_) (r *api.AppendEntriesReply, err error) {
	str := args.TopicName + args.PartName
	var logs []raft.Log
	json.Unmarshal(args.Log, &logs)

	p.mu.RLock()
	raft_ptr, ok := p.Partitions[str]
	p.mu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DWarn, "raft(%v) is not get\n", str)
		time.Sleep(time.Second * 10)
		return
	}

	resp := raft_ptr.AppendEntries(&raft.AppendEntriesArgs{
		Term:         int(args.Term),
		LeaderId:     int(args.LeaderId),
		PrevLogIndex: int(args.PrevLogIndex),
		PrevLogTerm:  int(args.PrevLogTerm),
		LeaderCommit: int(args.LeaderCommit),
		Log:          logs,
		LogIndex:     int(args.LogIndex),
	})
	return &api.AppendEntriesReply{
		Term:    int8(resp.Term),
		Success: resp.Success,
		XTerm:   int8(resp.XTerm),
		XIndex:  int8(resp.XIndex),
	}, nil
}

func (p *parts_raft) InstallSnapshot(ctx context.Context, args *api.InstallSnapshotArgs_) (r *api.InstallSnapshotReply, err error) {
	str := args.TopicName + args.PartName
	p.mu.RLock()
	raft_ptr, ok := p.Partitions[str]
	p.mu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DError, "raft(%v) is not get\n", str)
		time.Sleep(time.Second * 10)
		return
	}

	resp := raft_ptr.InstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              int(args.Term),
		LeaderId:          int(args.LeaderId),
		LastIncludedIndex: int(args.LastIncludedIndex),
		LastIncludedTerm:  int(args.LastIncludedTerm),
		Snapshot:          args.Snapshot,
	})

	return &api.InstallSnapshotReply{
		Term:    int8(resp.Term),
		Success: resp.Success,
	}, nil
}

func (p *parts_raft) Pingpongtest(ctx context.Context, req *api.PingPongArgs_) (r *api.PingPongReply, err error) {
	return &api.PingPongReply{
		Pong: true,
	}, nil
}
