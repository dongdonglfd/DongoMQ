package server

import (
	"DongoMQ/client/clients"
	"DongoMQ/kitex_gen/api"
	"DongoMQ/kitex_gen/api/server_operations"
	"DongoMQ/logger"
	"DongoMQ/zookeeper"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"

	"github.com/cloudwego/kitex/client"
)

type ZKServer struct {
	mu              sync.RWMutex
	zk              *zookeeper.ZK
	Name            string
	Info_Topics     map[string]zookeeper.TopicNode
	Info_Partitions map[string]zookeeper.PartitionNode

	Brokers map[string]server_operations.Client //连接各个Broker

	//每个partition所在的集群 topic+partition to brokers
	PartToBro map[string][]string

	consistent *ConsistentBro
}

type Info_in struct {
	cliName   string
	topicName string
	partName  string
	blockName string
	index     int64
	option    int8
	repNum    int8
}

type Info_out struct {
	Err         error
	brokerName  string
	broHostPort string
	Ret         string
}

func NewZKServer(zkinfo zookeeper.ZkInfo) *ZKServer {
	zkServer := new(ZKServer)
	zkServer.zk, _ = zookeeper.NewZK(zkinfo)
	zkServer.mu = sync.RWMutex{}

	return zkServer
}

func (z *ZKServer) make(opt Options) {
	z.Name = opt.Name
	z.Info_Topics = make(map[string]zookeeper.TopicNode)
	z.Info_Partitions = make(map[string]zookeeper.PartitionNode)
	z.Brokers = make(map[string]server_operations.Client)
	z.PartToBro = make(map[string][]string)

	z.consistent = NewConsistentBro()
}

// broker连接到zkserver，zkserver将在zookeeper上监听broker的状态
func (z *ZKServer) HandleBroInfo(bro_name, bro_H_P string) error {
	bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(bro_H_P))
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}
	z.mu.Lock()
	z.Brokers[bro_name] = bro_cli
	z.mu.Unlock()

	//加入consistent中进行负载均衡
	z.consistent.Add(bro_name, 1)

	//增加动态扩容机制，若有新broker加入应动态进行扩容
	//既对一些服务进行动态转移----producer服务
	//对于raft集群将集群停止并开启负载均衡完后将重新启动这个集群
	//对于fetch机制同上，暂时停止服务后迁移副本broker，负载均衡

	//对于consumer服务保持正常状态，继续提供服务

	return nil
}

func (z *ZKServer) CreateTopic(info Info_in) Info_out {
	//可添加限制数量等操作
	tnode := zookeeper.TopicNode{Name: info.topicName}
	err := z.zk.RegisterNode(tnode)
	return Info_out{Err: err}
}

// 创建Partition
func (z *ZKServer) CreatePart(info Info_in) Info_out {
	//可添加限制数量等操作
	pnode := zookeeper.PartitionNode{
		Name:      info.partName,
		Index:     int64(1),
		TopicName: info.topicName,
		Option:    -2,
		PTPoffset: int64(0),
	}

	err := z.zk.RegisterNode(pnode)
	if err != nil {
		return Info_out{Err: err}
	}

	//创建NowBlock节点，接收信息
	err = z.CreateNowBlock(info)
	return Info_out{Err: err}
}

func (z *ZKServer) CreateNowBlock(info Info_in) error {
	block_node := zookeeper.BlockNode{
		Name:        "NowBlock",
		FileName:    "NowBlock.txt",
		TopicName:   info.topicName,
		PartName:    info.partName,
		StartOffset: int64(0),
	}
	return z.zk.RegisterNode(block_node)
}

// 设置Partition的接收信息方式
// 若ack = -1,则为raft同步信息
// 若ack = 1,则leader写入,fetch获取信息
// 若ack = 0,则立刻返回，fetch获取信息
func (z *ZKServer) SetPartitionState(info Info_in) Info_out {
	var ret string
	var data_brokers []byte
	var reps []zookeeper.ReplicaNode
	node, err := z.zk.GetPartState(info.topicName, info.partName)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{Err: err}
	}

	if info.option != node.Option {
		index, err := z.zk.GetPartBlockIndex(info.topicName, info.partName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err)
			return Info_out{Err: err}
		}
		z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
			TopicName: info.topicName,
			Name:      info.partName,
			Index:     index,
			Option:    info.option,
			PTPoffset: node.PTPoffset,
			RepNum:    info.repNum, //需要下面的程序确认，是否能分配一定数量的副本
		})
	}

	logger.DEBUG(logger.DLog, "this partition(%v) status is %v\n", node.Name, node.Option)

	if node.Option == -2 {
		//未创建任何状态，即该partition未接收过任何消息

		switch info.option {
		case -1:
			//raft副本个数暂时默认3个
			reps, data_brokers = z.GetRepsFromConsist(info)
			if len(reps) == 0 {
				return Info_out{Err: errors.New("no broker replica available")}
			}
			err = z.BecomeLeader(Info_in{
				cliName:   reps[0].BrokerName,
				topicName: info.topicName,
				partName:  info.partName,
			})
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return Info_out{Err: err}
			}

			//向这些broker发送信息，启动raft
			for _, repNode := range reps {
				bro_cli, ok := z.Brokers[repNode.BrokerName]
				if !ok {
					logger.DEBUG(logger.DLog, "this partition(%v) leader broker is not connected\n", info.partName)
				} else {
					resp, err := bro_cli.AddRaftPartition(context.Background(), &api.AddRaftPartitionRequest{
						TopicName: info.topicName,
						PartName:  info.partName,
						Brokers:   data_brokers,
					})

					logger.DEBUG(logger.DLog, "the broker %v had add raft\n", repNode.BrokerName)
					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp, err.Error())
						return Info_out{Err: err}
					}
				}
			}
		default:
			//负载均衡获得一定数量broker节点，选择一个leader，并让其他节点fetch leader消息
			//默认副本数为3

			reps, data_brokers = z.GetRepsFromConsist(info)
			if len(reps) == 0 {
				return Info_out{Err: errors.New("no broker replica available")}
			}

			//选择一个broker节点作为leader
			LeaderBroker, err := z.zk.GetBrokerNode(reps[0].BrokerName)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return Info_out{Err: err}
			}

			//更新nowblock中的leader
			err = z.BecomeLeader(Info_in{
				cliName:   reps[0].BrokerName,
				topicName: info.topicName,
				partName:  info.partName,
			})

			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return Info_out{Err: err}
			}

			for _, repNode := range reps {
				bro_cli, ok := z.Brokers[repNode.BrokerName]
				if !ok {
					logger.DEBUG(logger.DError, "this partition(%v) leader broker is not connected\n", info.partName)
				} else {
					//开启fetch机制
					resp3, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topicName,
						PartName:     info.partName,
						HostPort:     LeaderBroker.BrokHostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     "NowBlock.txt",
						Brokers:      data_brokers,
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp3, err.Error())
						return Info_out{Err: err}
					}
				}
			}
		}

		return Info_out{
			Ret: ret,
		}
	}

	//获取该partition
	LeaderBroker, NowBlock, err := z.zk.GetPartNowBrokerNode(info.topicName, info.partName)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{Err: err}
	}
	reps = z.zk.GetReplicaNodes(NowBlock.TopicName, NowBlock.PartName, NowBlock.Name)

	var brokers BrokerS
	brokers.BroBrokers = make(map[string]string)
	brokers.RafBrokers = make(map[string]string)
	for _, repNode := range reps {
		BrokerNode, err := z.zk.GetBrokerNode(repNode.BrokerName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
		brokers.BroBrokers[repNode.BrokerName] = BrokerNode.BrokHostPort
		brokers.RafBrokers[repNode.BrokerName] = BrokerNode.RaftHostPort
	}

	data_brokers, err = json.Marshal(brokers)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{Err: err}
	}

	switch info.option {
	case -1:
		if node.Option == -1 { //与原来的状态相同
			ret = "HadRaft"
		}
		if node.Option == 1 || node.Option == 0 { //原状态为fetch，关闭原来的写状态，创建新raft集群
			//查询raft集群的broker，发送信息
			//fetch操作继续同步之前文件，创建raft集群，需要更换新文件写入
			//调用CloseAcceptPartition更换文件
			for ice, repNode := range reps {
				//停止接收该Partition的信息，更换一个新文件写入信息，因为fetch机制一些信息已经写入leader
				//但未写入follower中，更换文件从头写入，重新开启fetch机制为上一个文件同步信息
				newfilename := z.CloseAcceptPartition(info.topicName, info.partName, repNode.BrokerName, ice)

				bro_cli, ok := z.Brokers[repNode.BrokerName]
				if !ok {
					logger.DEBUG(logger.DLog, "this partition(%v) leader broker is not connected\n", info.partName)
				} else {
					//关闭fetch机制
					resp1, err := bro_cli.CloseFetchPartition(context.Background(), &api.CloseFetchPartitionRequest{
						TopicName: info.topicName,
						PartName:  info.partName,
					})
					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp1, err.Error())
						return Info_out{Err: err}
					}

					//重新准备接收文件
					resp2, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
						TopicName: info.topicName,
						PartName:  info.partName,
						FileName:  "NowBlock.txt",
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp2, err.Error())
						return Info_out{Err: err}
					}

					//开启raft集群
					resp3, err := bro_cli.AddRaftPartition(context.Background(), &api.AddRaftPartitionRequest{
						TopicName: info.topicName,
						PartName:  info.partName,
						Brokers:   data_brokers,
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp3, err.Error())
						return Info_out{Err: err}
					}

					//开启fetch机制，同步完上一个文件
					resp4, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topicName,
						PartName:     info.partName,
						HostPort:     LeaderBroker.BrokHostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     newfilename,
						Brokers:      data_brokers,
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp4, err.Error())
						return Info_out{Err: err}
					}
				}
			}
		}
	default:
		if node.Option != -1 { //与原状态相同
			ret = "HadFetch"
		} else { //由raft改为fetch
			//查询fetch的broker，发送消息
			//关闭raft集群，开启fetch操作，不需要更换文件
			for _, repNode := range reps {
				//停止接收该Partition的信息，当raft被终止后broker server将不会接收该partition的信息
				//NowBlock的文件不需要关闭接收信息，启动fetch机制后，可以继续向该文件写入信息
				bro_cli, ok := z.Brokers[repNode.BrokerName]
				if !ok {
					logger.DEBUG(logger.DLog, "the partition(%v) leader broker is not connected\n", info.partName)
				} else {
					//关闭raft集群
					resp1, err := bro_cli.CloseRaftPartition(context.Background(), &api.CloseRaftPartitionRequest{
						TopicName: info.topicName,
						PartName:  info.partName,
					})
					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp1, err.Error())
						return Info_out{Err: err}
					}

					//开启fetch机制
					resp2, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topicName,
						PartName:     info.partName,
						HostPort:     LeaderBroker.BrokHostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     "NowBlock.txt",
						Brokers:      data_brokers,
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp2, err.Error())
						return Info_out{Err: err}
					}
				}
			}
		}
	}
	return Info_out{
		Ret: ret,
	}
}

// 需要向每个副本都发送
// 发送请求到broker，关闭该broker上的partition的接收程序
// 并修改NowBlock的文件名，并修改zookeeper上的block信息
func (z *ZKServer) CloseAcceptPartition(topicName, partName, brokerName string, ice int) string {
	//获取新文件名
	index, err := z.zk.GetPartBlockIndex(topicName, partName)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err.Error()
	}
	NewBlockName := "Block_" + strconv.Itoa(int(index))
	NewFileName := NewBlockName + ".txt"

	z.mu.RLock()
	bro_cli, ok := z.Brokers[brokerName]
	if !ok {
		logger.DEBUG(logger.DError, "broker(%v) is not connected\n", brokerName)
	} else {
		resp, err := bro_cli.CloseAccept(context.Background(), &api.CloseAcceptRequest{
			TopicName:    topicName,
			PartName:     partName,
			Oldfilename:  "NowBlock.txt",
			Newfilename_: NewFileName,
		})
		if err != nil || !resp.Ret {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		} else {
			str := z.zk.TopicRoot + "/" + topicName + "/Partitions/" + partName + "/" + "NowBlock"
			bnode, err := z.zk.GetBlockNode(str)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}

			if ice == 0 {
				//创建新节点
				z.zk.RegisterNode(zookeeper.BlockNode{
					Name:         NewBlockName,
					TopicName:    topicName,
					PartName:     partName,
					FileName:     NewFileName,
					StartOffset:  resp.Startindex,
					EndOffset:    resp.Endindex,
					LeaderBroker: bnode.LeaderBroker,
				})

				//更新原NowBlock节点信息
				z.zk.UpdateBlockNode(zookeeper.BlockNode{
					Name:        "NowBlock",
					TopicName:   topicName,
					PartName:    partName,
					FileName:    "NowBlock.txt",
					StartOffset: resp.Endindex + 1,
					//leader暂时未选出
				})
			}

			//创建该节点下的各个Rep节点
			RepPath := z.zk.TopicRoot + "/" + topicName + "/Partitions/" + partName + "/" + "NowBlock" + "/" + brokerName
			RepNode, err := z.zk.GetReplicaNode(RepPath)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}

			RepNode.BlockName = NewBlockName

			z.zk.RegisterNode(RepNode) //在NewBlock上创建副本节点
		}
	}
	z.mu.RUnlock()

	return NewFileName
}

func (z *ZKServer) GetRepsFromConsist(info Info_in) (Reps []zookeeper.ReplicaNode, data_brokers []byte) {
	str := info.topicName + info.partName
	Bro_reps := z.consistent.GetNode(str+"rep", 3)
	for index, brokerName := range Bro_reps {
		if brokerName == "" {
			continue
		}
		Reps = append(Reps, zookeeper.ReplicaNode{
			Name:        "rep_" + strconv.Itoa(index),
			TopicName:   info.topicName,
			PartName:    info.partName,
			BrokerName:  brokerName,
			StartOffset: int64(0),
			BlockName:   "NowBlock",
		})
	}

	for _, rep := range Reps {
		err := z.zk.RegisterNode(rep)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
	}

	var brokers BrokerS
	brokers.BroBrokers = make(map[string]string)
	brokers.RafBrokers = make(map[string]string)
	brokers.Me_Brokers = make(map[string]int)
	for _, repNode := range Reps {
		BrokerNode, err := z.zk.GetBrokerNode(repNode.BrokerName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
		brokers.BroBrokers[repNode.BrokerName] = BrokerNode.BrokHostPort
		brokers.RafBrokers[repNode.BrokerName] = BrokerNode.RaftHostPort
		brokers.Me_Brokers[repNode.BrokerName] = BrokerNode.Me
	}

	data_brokers, err := json.Marshal(brokers)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	return Reps, data_brokers
}

func (z *ZKServer) BecomeLeader(info Info_in) error {
	logger.DEBUG(logger.DLeader, "partition(%v) new leader is %v\n", info.topicName+info.partName, info.cliName)
	now_block_path := z.zk.TopicRoot + "/" + info.topicName + "/Partitions/" + info.partName + "/NowBlock"
	NowBlock, err := z.zk.GetBlockNode(now_block_path)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	NowBlock.LeaderBroker = info.cliName
	return z.zk.UpdateBlockNode(NowBlock)
}

// producer get broker
func (z *ZKServer) ProGetLeader(info Info_in) Info_out {
	//查询zookeeper,获得leaderBroker的host_port，若未连接则建立连接
	broker, block, err := z.zk.GetPartNowBrokerNode(info.topicName, info.partName)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{Err: err}
	}
	PartitionNode, err := z.zk.GetPartState(info.topicName, info.partName)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{Err: err}
	}

	//检查该Partition的状态是否设定
	//检查该Partition在Brokers上是否创建raft集群或fetch
	Brokers := make(map[string]string)
	var ret string
	replicas := z.zk.GetReplicaNodes(block.TopicName, block.PartName, block.Name)
	for _, replica := range replicas {
		BrokerNode, err := z.zk.GetBrokerNode(replica.BrokerName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
		Brokers[replica.BrokerName] = BrokerNode.BrokHostPort
	}

	data, err := json.Marshal(Brokers)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	for BrokerName, BrokerHostPort := range Brokers {
		z.mu.RLock()
		bro_cli, ok := z.Brokers[BrokerName]
		z.mu.RUnlock()

		//未连接该broker
		if !ok {
			bro_cli, err = server_operations.NewClient(z.Name, client.WithHostPorts(BrokerHostPort))
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
			z.mu.Lock()
			z.Brokers[BrokerName] = bro_cli
			z.mu.Unlock()
		}

		// 通知broker检查topic/partition，并创建队列准备接收信息
		resp, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
			TopicName: block.TopicName,
			PartName:  block.PartName,
			FileName:  block.FileName,
		})
		if err != nil || !resp.Ret {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}

		//检查该Partition的状态是否恒定
		//检查该Partition在brokers上是否创建raft集群或fetch
		//若该Partition没有设置状态则返回通知producer
		if PartitionNode.Option == -2 { //未设置状态
			ret = "Partition State is -2"
		} else {
			resp, err := bro_cli.PrepareState(context.Background(), &api.PrepareStateRequest{
				TopicName: block.TopicName,
				PartName:  block.PartName,
				State:     PartitionNode.Option,
				Brokers:   data,
			})
			if err != nil || !resp.Ret {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
		}
	}

	//返回producer broker和host_port
	return Info_out{
		Err:         err,
		brokerName:  broker.Name,
		broHostPort: broker.BrokHostPort,
		Ret:         ret,
	}
}

func (z *ZKServer) SubHandle(info Info_in) error {
	//在zookeeper上创建sub节点，若节点已经存在，则加入group中
	path := fmt.Sprintf(zookeeper.PNodePath, z.zk.TopicRoot, info.topicName, info.partName)
	_, err := z.zk.GetPartitionNode(path)
	if err != nil {
		logger.DEBUG(logger.DError, "the topic-partition(%v-%v) does not exist\n", info.topicName, info.partName)
		return errors.New("the topic-partition does not exist")
	}
	path += "/NowBlock"
	replicas, _, _ := z.zk.Conn.Children(path)
	for _, replica := range replicas {
		replicaNode, err := z.zk.GetReplicaNode(path + "/" + replica)
		if err != nil {
			logger.DEBUG(logger.DError, "get replica node %v failed %v\n", path+"/"+replica, err.Error())
			return err
		}
		bro_cli, ok := z.Brokers[replicaNode.BrokerName]
		if !ok {
			brokerNode, err := z.zk.GetBrokerNode(replicaNode.BrokerName)
			if err != nil {
				logger.DEBUG(logger.DError, "get broker(%v) failed %v\n", replicaNode.BrokerName, err.Error())
				return err
			}
			bro_cli, err = server_operations.NewClient(z.Name, client.WithHostPorts(brokerNode.BrokHostPort))
			if err != nil {
				logger.DEBUG(logger.DError, "broker(%v) host_port(%v) can't connect %v", brokerNode.Name, brokerNode.BrokHostPort, err.Error())
				return err
			}
			z.mu.Lock()
			z.Brokers[brokerNode.Name] = bro_cli
			z.mu.Unlock()
		}
		resp, err := bro_cli.Sub2(context.Background(), &api.Sub2Request{
			Consumer: info.cliName,
			Topic:    info.topicName,
			Key:      info.partName,
			Option:   info.option,
		})
		if err != nil || !resp.Ret {
			if err != nil {
				logger.DEBUG(logger.DError, "server sub failed %v\n", err.Error())
			} else {
				logger.DEBUG(logger.DError, "server sub failed: broker returned false\n")
			}
			return errors.New("server sub fail")
		}
	}
	err = z.zk.RegisterNode(zookeeper.SubscriptionNode{
		Name:      info.cliName,
		TopicName: info.topicName,
		PartName:  info.partName,
		Subtype:   info.option,
	})
	if err != nil {
		return err
	}
	return nil
}

// consumer查询该向哪些broker发送请求
// zkserver让broker准备好topic/sub和config
func (z *ZKServer) HandStartGetBroker(info Info_in) (rets []byte, size int, err error) {
	var Parts []zookeeper.Part

	//检查该用户是否订阅了该topic/partition
	z.zk.CheckSub(zookeeper.StartGetInfo{
		CliName:       info.cliName,
		TopicName:     info.topicName,
		PartitionName: info.partName,
		Option:        info.option,
	})

	//获取该topic或partition的broker，并保证在线，若全部离线则Err
	if info.option == TOPIC_NIL_PTP_PULL || info.option == TOPIC_NIL_PTP_PUSH { //ptp_push
		Parts, err = z.zk.GetBrokers(info.topicName)
	} else if info.option == TOPIC_KEY_PSB_PULL || info.option == TOPIC_KEY_PSB_PUSH { //psb_push
		Parts, err = z.zk.GetBroker(info.topicName, info.partName, info.index)
	}
	if err != nil {
		return nil, 0, err
	}
	logger.DEBUG(logger.DLog, "the brokers are %v\n", Parts)

	//获取到信息后将通知brokers，让他们检查是否有该topic/partition/subscription/config等
	//并开启part发送协程，若协程在超时时间到后未收到管道的信息，则关闭协程
	partkeys := z.SendPreoare(Parts, info)

	parts := clients.Parts{
		PartKeys: partkeys,
	}
	data, err := json.Marshal(parts)

	var nodes clients.Parts

	json.Unmarshal(data, &nodes)

	logger.DEBUG(logger.DLog, "the partkeys %v and nodes %v\n", partkeys, nodes)
	if err != nil {
		logger.DEBUG(logger.DError, "turn partkeys to json failed\n", err.Error())
	}

	return data, size, nil
}

func (z *ZKServer) SendPreoare(Parts []zookeeper.Part, info Info_in) (partkeys []clients.PartKey) {
	for _, part := range Parts {
		if part.Err != OK {
			logger.DEBUG(logger.DLog, "the part.ERR(%v) != OK the part is %v\n", part.Err, part)
			partkeys = append(partkeys, clients.PartKey{
				Err: part.Err,
			})
			continue
		}
		z.mu.RLock()
		bro_cli, ok := z.Brokers[part.BrokerName]
		z.mu.RUnlock()

		if !ok {
			bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(part.BrokHost_Port))
			if err != nil {
				logger.DEBUG(logger.DError, "broker(%v) host_port(%v) can't connect %v", part.BrokerName, part.BrokHost_Port, err.Error())
			}
			z.mu.Lock()
			z.Brokers[part.BrokerName] = bro_cli
			z.mu.Unlock()
		}
		rep := &api.PrepareSendRequest{
			Consumer:  info.cliName,
			TopicName: info.topicName,
			PartName:  info.partName,
			FileName:  part.Filename,
			Option:    info.option,
		}
		if rep.Option == TOPIC_NIL_PTP_PULL || rep.Option == TOPIC_NIL_PTP_PUSH { //ptp
			rep.Offset = part.PTP_index
		} else if rep.Option == TOPIC_KEY_PSB_PULL || rep.Option == TOPIC_KEY_PSB_PUSH { //psb
			rep.Offset = info.index
		}
		resp, err := bro_cli.PrepareSend(context.Background(), rep)
		if err != nil || !resp.Ret {
			logger.DEBUG(logger.DError, "PrepareSend error %v\n", err.Error())
		}
		logger.DEBUG(logger.DLog, "the part is %v", part)
		partkeys = append(partkeys, clients.PartKey{
			Name:       part.PartName,
			BrokerName: part.BrokerName,
			Broker_H_P: part.BrokHost_Port,
			Offset:     part.PTP_index,
			Err:        part.Err,
		})
	}
	return partkeys
}

// Broker为节点
type ConsistentBro struct {
	//排序的hash虚拟节点(环形)
	hashSortedNodes []uint32
	//虚拟节点(broker)对应的实际节点
	circle map[uint32]string
	//已绑定的broker为true
	nodes map[string]bool

	//用于跟踪Broker节点的临时状态（是否已被选中）
	BroH map[string]bool

	mu sync.RWMutex
	//虚拟节点个数
	virtualNodeCount int
}

func NewConsistentBro() *ConsistentBro {
	con := &ConsistentBro{
		hashSortedNodes: make([]uint32, 0),
		circle:          make(map[uint32]string),
		nodes:           make(map[string]bool),
		BroH:            make(map[string]bool),

		mu:               sync.RWMutex{},
		virtualNodeCount: VIRTUAL_10,
	}

	return con
}

func (c *ConsistentBro) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// add broker name as node
func (c *ConsistentBro) Add(node string, power int) error {
	if node == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.nodes[node]; ok {
		return errors.New("node already existed")
	}
	c.nodes[node] = true

	for i := 0; i < c.virtualNodeCount*power; i++ {
		virtualKey := c.hashKey(node + strconv.Itoa(i))
		c.circle[virtualKey] = node
		c.hashSortedNodes = append(c.hashSortedNodes, virtualKey)
	}

	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})

	return nil
}

func (c *ConsistentBro) SetBroHFalse() {
	for Bro := range c.BroH {
		c.BroH[Bro] = false
	}
}

// return broker name
func (c *ConsistentBro) GetNode(key string, num int) (reps []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if num <= 0 || len(c.nodes) == 0 || len(c.hashSortedNodes) == 0 {
		return nil
	}
	if num > len(c.nodes) {
		num = len(c.nodes)
	}

	c.SetBroHFalse()

	hash := c.hashKey(key)
	for index := 0; index < num; index++ {
		i := c.getPosition(hash)
		if i < 0 {
			break
		}

		broker_name := c.circle[c.hashSortedNodes[i]]

		reps = append(reps, broker_name)

		c.BroH[broker_name] = true
	}

	return reps
}

func (c *ConsistentBro) getPosition(hash uint32) (ret int) {
	if len(c.hashSortedNodes) == 0 {
		return -1
	}
	i := sort.Search(len(c.hashSortedNodes), func(i int) bool {
		return c.hashSortedNodes[i] >= hash
	})

	if i == len(c.hashSortedNodes) {
		i = 0
	}

	for scanned := 0; scanned < len(c.hashSortedNodes); scanned++ {
		ret = (i + scanned) % len(c.hashSortedNodes)
		broker := c.circle[c.hashSortedNodes[ret]]
		if broker != "" && !c.BroH[broker] {
			return ret
		}
	}
	return -1
}

func (z *ZKServer) UpdateRepNode(info Info_in) error {
	if info.topicName == "" || info.partName == "" || info.blockName == "" || info.cliName == "" {
		return fmt.Errorf("invalid replica update: topic=%q part=%q block=%q broker=%q", info.topicName, info.partName, info.blockName, info.cliName)
	}

	str := z.zk.TopicRoot + "/" + info.topicName + "/Partitions/" + info.partName + "/" + info.blockName
	BlockNode, err := z.zk.GetBlockNode(str)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}
	if info.index > BlockNode.EndOffset {
		BlockNode.EndOffset = info.index
		err = z.zk.RegisterNode(BlockNode)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return err
		}
	}

	RepNode, err := z.zk.GetReplicaNode(str + "/" + info.cliName)
	if err != nil || RepNode.Name == "" {
		RepNode, err = z.findReplicaByBroker(info.topicName, info.partName, info.blockName, info.cliName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return err
		}
	}
	RepNode.EndOffset = info.index
	err = z.zk.RegisterNode(RepNode)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}
	return nil
}

func (z *ZKServer) findReplicaByBroker(topicName, partName, blockName, brokerName string) (zookeeper.ReplicaNode, error) {
	for _, rep := range z.zk.GetReplicaNodes(topicName, partName, blockName) {
		if rep.BrokerName == brokerName {
			return rep, nil
		}
	}
	return zookeeper.ReplicaNode{}, fmt.Errorf("replica for broker %q not found at %s/%s/%s", brokerName, topicName, partName, blockName)
}

// 修改Offset
func (z *ZKServer) UpdatePTPOffset(info Info_in) error {
	str := z.zk.TopicRoot + "/" + info.topicName + "/Partitions/" + info.partName
	node, err := z.zk.GetPartitionNode(str)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}

	err = z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
		Name:      info.partName,
		TopicName: info.topicName,
		Index:     node.Index,
		Option:    node.Option,
		RepNum:    node.RepNum,
		PTPoffset: info.index,
	})
	return err
}

func (z *ZKServer) GetNewLeader(info Info_in) (Info_out, error) {
	block_path := z.zk.TopicRoot + "/" + info.topicName + "/Partitions/" + info.partName + "/" + info.blockName
	logger.DEBUG(logger.DLog, "get new leader broker the path is %v\n", block_path)
	BlockNode, err := z.zk.GetBlockNode(block_path)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	var leaderBroker zookeeper.BrokerNode
	//需要检查Leader是否在线，若不在线需要更换leader
	logger.DEBUG(logger.DLog, "zkserver checkout leader broker %v online?\n", BlockNode.LeaderBroker)
	ret := z.zk.CheckBroker(BlockNode.LeaderBroker)
	if ret {
		leaderBroker, err = z.zk.GetBrokerNode(BlockNode.LeaderBroker)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
	} else {
		//检查副本中谁的最新，再次检查
		var array []struct {
			EndIndex   int64
			BrokerName string
		}
		Reps := z.zk.GetReplicaNodes(info.topicName, info.partName, info.blockName)
		for _, rep := range Reps {
			ret = z.zk.CheckBroker(rep.BrokerName)
			if ret {
				//根据EndIndex的大小排序
				array = append(array, struct {
					EndIndex   int64
					BrokerName string
				}{rep.EndOffset, rep.BrokerName})
			}
		}

		sort.SliceStable(array, func(i, j int) bool {
			return array[i].EndIndex > array[j].EndIndex
		})

		for _, arr := range array {
			leaderBroker, err = z.zk.GetBrokerNode(arr.BrokerName)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
			ret = z.zk.CheckBroker(arr.BrokerName)
			if ret {
				break
			}
		}
	}

	return Info_out{
		brokerName:  leaderBroker.Name,
		broHostPort: leaderBroker.BrokHostPort,
	}, nil
}
