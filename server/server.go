package server

import (
	"DongoMQ/kitex_gen/api"
	"DongoMQ/kitex_gen/api/client_operations"
	"DongoMQ/kitex_gen/api/raft_operations"
	"DongoMQ/kitex_gen/api/server_operations"
	"DongoMQ/kitex_gen/api/zkserver_operations"
	"DongoMQ/logger"
	"DongoMQ/zookeeper"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
)

const (
	NODE_SIZE = 24
)

type Server struct {
	Name     string
	me       int
	zk       *zookeeper.ZK
	zkclient zkserver_operations.Client
	mu       sync.RWMutex

	appench   chan info
	topics    map[string]*Topic
	consumers map[string]*Client
	brokers   map[string]*raft_operations.Client

	//raft
	parts_rafts *parts_raft

	//fetch
	parts_fetch   map[string]string                    //topicName + partitionName + filename to leaderBrokerName,是否开启fetch标志
	brokers_fetch map[string]*server_operations.Client //brokerName to Client
}

type info struct {
	topicName string
	partName  string
	fileName  string
	newName   string
	option    int8
	offset    int64
	size      int8

	ack int8

	producer string
	consumer string
	cmdIndex int64
	message  []byte

	//raft
	brokers map[string]string
	brok_me map[string]int
	me      int

	//fetch
	LeaderBroker string
	HostPort     string

	//update rep
	zkclient   *zkserver_operations.Client
	BrokerName string
}

func NewServer(zkinfo zookeeper.ZkInfo) *Server {
	server := &Server{}
	server.zk, _ = zookeeper.NewZK(zkinfo) //连接上zookeeper
	server.mu = sync.RWMutex{}

	return server
}

func (s *Server) Make(opt Options, opt_cli []server.Option) {
	s.consumers = make(map[string]*Client)
	s.topics = make(map[string]*Topic)
	s.brokers = make(map[string]*raft_operations.Client)
	s.parts_fetch = make(map[string]string)
	s.brokers_fetch = make(map[string]*server_operations.Client)
	s.appench = make(chan info)

	s.CheckList()
	s.Name = opt.Name
	s.me = opt.Me

	//本地创建parts-raft，为raft同步做准备
	s.parts_rafts = NewParts_Raft()
	s.parts_rafts.Make(opt.Name, opt_cli, s.appench, s.me)
	s.parts_rafts.StartServer()

	//在zookeeper上创建一个永久节点，若存在则不需要创建
	err := s.zk.RegisterNode(zookeeper.BrokerNode{
		Name:         s.Name,
		Me:           s.me,
		BrokHostPort: opt.Broker_Host_Port,
		RaftHostPort: opt.Raft_Host_Port,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	//创建临时节点，用于zkserver的watch
	err = s.zk.CreateState(s.Name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	//连接zkServer，并将自己的info发送到zkServer
	zkclient, err := zkserver_operations.NewClient(opt.Name, client.WithHostPorts(opt.ZKServer_Host_Port))
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	s.zkclient = zkclient

	resp, err := zkclient.BroInfo(context.Background(), &api.BroInfoRequest{
		BrokerName:     opt.Name,
		BrokerHostPort: opt.Broker_Host_Port,
	})
	if err != nil || !resp.Ret {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	//开始获取管道中的内容，写入文件或更新leader
	go s.GetApplych(s.appench)
}

// 接收applych管道的内容
// 写入partition文件中
func (s *Server) GetApplych(appench chan info) {
	for {
		select {
		case msg := <-appench:
			if msg.producer == "Leader" {
				s.BecomeLeader(msg) //成为leader
			} else {
				s.mu.RLock()
				topic, ok := s.topics[msg.topicName]
				s.mu.RUnlock()

				logger.DEBUG(logger.DLog, "%d the message from applych is %v\n", s.me, msg)
				if !ok {
					logger.DEBUG(logger.DError, "topic(%v) is not in this broker\n", msg.topicName)
				} else {
					msg.me = s.me
					msg.BrokerName = s.Name
					msg.zkclient = &s.zkclient
					msg.fileName = "NowBlock.txt"
					topic.addMessage(msg) //信息同步
				}
			}
		}
	}
}

func (s *Server) BecomeLeader(in info) {
	resp, err := s.zkclient.BecomeLeader(context.Background(), &api.BecomeLeaderRequest{
		Broker:    s.Name,
		Topic:     in.topicName,
		Partition: in.partName,
	})
	if err != nil || !resp.Ret {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
}

func (s *Server) CheckList() {
	str, _ := os.Getwd()
	str += "/" + s.Name
	ret := CheckFileOrList(str)
	if !ret {
		CreateList(str)
	}
}

func (s *Server) PrepareState(in info) (ret string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch in.option {
	case -1:
		ok := s.parts_rafts.CheckPartState(in.topicName, in.partName)
		if !ok {
			ret = "the raft not exists"
			err = errors.New(ret)
		}
	default:

	}
	return ret, err
}

// 准备接收信息
// 检查topic和partition是否存在，不存在则需要创建
// 设置partition中的file和fd,start_index等信息
func (s *Server) PrepareAcceptHandle(in info) (ret string, err error) {
	//检查topic
	s.mu.Lock()
	topic, ok := s.topics[in.topicName]
	if !ok {
		topic = NewTopic(s.Name, in.topicName)
		if topic == nil {
			return "create topic failed", errors.New("create topic failed")
		}
		s.topics[in.topicName] = topic
	}

	s.mu.Unlock()

	//检查partition
	return topic.PrepareAcceptHandle(in)
}

// 停止接收文件，并将文件名修改成newfilename
// 指NowBlock中的Leader停止接收信息，副本可继续Pull信息，当EOF后关闭
func (s *Server) CloseAcceptHandle(in info) (start, end int64, ret string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	topic, ok := s.topics[in.topicName]
	if !ok {
		ret = "this topic is not in this broker"
		logger.DEBUG(logger.DError, "this topic(%v) is not in this broker\n", in.topicName)
		return 0, 0, ret, errors.New(ret)
	}
	return topic.CloseAcceptPart(in)
}

// PrepareSendHandle 准备发送消息
// 检查topic和subscription是否存在，不存在则需要创建
// 检查该文件的config是否存在，不存在则创建，并开启线程
// 协程设置超时时间，时间到则关闭
func (s *Server) PrepareSendHandle(in info) (ret string, err error) {
	//检查或创建topic
	s.mu.Lock()
	topic, ok := s.topics[in.topicName]
	if !ok {
		logger.DEBUG(logger.DLog, "%v not have topic(%v),create topic\n", s.Name, in.topicName)
		topic = NewTopic(s.Name, in.topicName)
		if topic == nil {
			return "create topic failed", errors.New("create topic failed")
		}
		s.topics[in.topicName] = topic
	}
	s.mu.Unlock()

	//检查或创建partition
	return topic.PrepareSendHandle(in, &s.zkclient)
}

func (s *Server) AddRaftHandle(in info) (ret string, err error) {
	//检测该Partition的Raft是否已经启动
	s.mu.Lock()
	defer s.mu.Unlock()
	index := 0
	nodes := make(map[int]string)
	for k, v := range in.brok_me {
		nodes[v] = k
	}

	var peers []*raft_operations.Client
	for index < len(in.brokers) {
		logger.DEBUG(logger.DLog, "%v index (%v) Me(%v) k(%v) == Name(%v)\n", s.Name, index, index, nodes[index], s.Name)
		bro_cli, ok := s.brokers[nodes[index]]
		if !ok {
			cli, err := raft_operations.NewClient(s.Name, client.WithHostPorts(in.brokers[nodes[index]]))
			if err != nil {
				logger.DEBUG(logger.DError, "%v new raft client fail err %v\n", s.Name, err.Error())
				return ret, err
			}
			s.brokers[nodes[index]] = &cli
			bro_cli = &cli
			logger.DEBUG(logger.DLog, "%v new client to broker %v\n", s.Name, nodes[index])
		} else {
			logger.DEBUG(logger.DLog, "%v new client to broker %v\n", s.Name, nodes[index])
		}
		peers = append(peers, bro_cli)
		index++
	}

	logger.DEBUG(logger.DLog, "the Broker %v raft Me %v\n", s.Name, s.me)
	//检查或创建底层part_raft
	s.parts_rafts.AddPart_Raft(peers, s.me, in.topicName, in.partName)

	logger.DEBUG(logger.DLog, "the %v add over\n", s.Name)

	return ret, err
}

func (s *Server) CloseRaftHandle(in info) (ret string, err error) {
	s.mu.RLock()
	err = s.parts_rafts.DeletePart_raft(in.topicName, in.partName)
	s.mu.RUnlock()
	if err != nil {
		return err.Error(), err
	}
	return ret, err
}

func (s *Server) AddFetchHandle(in info) (ret string, err error) {
	//检查该Partition的fetch机制是否已经启动

	//检查该topic_partition是否准备好accept信息
	ret, err = s.PrepareAcceptHandle(in)
	if err != nil {
		logger.DEBUG(logger.DError, "%v err is %v\n", ret, err)
		return ret, err
	}

	if in.LeaderBroker == s.Name {
		//Leader Broker将准备好接收follower的Pull请求
		s.mu.RLock()
		defer s.mu.RUnlock()
		topic, ok := s.topics[in.topicName]
		if !ok {
			ret := "this topic is not in this broker"
			logger.DEBUG(logger.DError, "%v,info(%v)\n", ret, in)
			return ret, errors.New(ret)
		}
		logger.DEBUG(logger.DLog, "%v prepare send for follower brokers\n", s.Name)
		//给每个follower broker准备node(PSB_PULL),等待pull请求
		for BrokerName := range in.brokers {
			ret, err = topic.PrepareSendHandle(info{
				topicName: in.topicName,
				partName:  in.partName,
				fileName:  in.fileName,
				consumer:  BrokerName,
				option:    TOPIC_KEY_PSB_PULL, //PSB_PULL
			}, &s.zkclient)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
		}
		return ret, err
	} else {
		time.Sleep(time.Microsecond * 100)
		str := in.topicName + in.partName + in.fileName
		s.mu.Lock()
		defer s.mu.Unlock()
		broker, ok := s.brokers_fetch[in.LeaderBroker]
		if !ok {
			logger.DEBUG(logger.DLog, "%v connection the leader broker %v the HP(%v)\n", s.Name, in.LeaderBroker, in.HostPort)
			bro_ptr, err := server_operations.NewClient(s.Name, client.WithHostPorts(in.HostPort))
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return err.Error(), err
			}
			s.brokers_fetch[in.LeaderBroker] = &bro_ptr
			broker = &bro_ptr
		}

		_, ok = s.parts_fetch[str]
		if !ok {
			s.parts_fetch[str] = in.LeaderBroker
		}
		topic, ok := s.topics[in.topicName]
		if !ok {
			ret := "this topic is not in this broker"
			logger.DEBUG(logger.DLog, "%v,info(%v)\n", ret, in)
			return ret, errors.New(ret)
		}

		return s.FetchMsg(in, broker, topic)
	}
}

func (s *Server) CloseFetchHandle(in info) (ret string, err error) {
	str := in.topicName + in.partName + in.fileName
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.parts_fetch[str]
	if !ok {
		ret := "this topic-partition is not in this broker"
		logger.DEBUG(logger.DError, "this topic(%v)-partition(%v) is not in this broker\n", in.topicName, in.partName)
		return ret, errors.New(ret)
	} else {
		//关闭NowBlock的fetch机制，将NowBlock更改，需要重新为之前的Block开启fetch机制
		//直到EOF退出
		delete(s.parts_fetch, str)
		return ret, err
	}
}

// subscribe 订阅
func (s *Server) SubHandle(in info) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	topic, ok := s.topics[in.topicName]
	if !ok {
		topic = NewTopic(s.Name, in.topicName)
		if topic == nil {
			return errors.New("create topic failed")
		}
		s.topics[in.topicName] = topic
	}
	_, err = topic.AddSubscription(in)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) UnSubHandle(in info) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	topic, ok := s.topics[in.topicName]
	if !ok {
		return errors.New("this topic is not in this broker")
	}
	sub_name, err := topic.ReduceSubscription(in)
	if err != nil {
		s.consumers[in.consumer].ReduceSubscription(sub_name)
	}
	return nil
}

// start到该partition中的raft集群中
// 收到返回后判断该写入还是返回
func (s *Server) PushHandle(in info) (ret string, err error) {
	logger.DEBUG(logger.DLog, "get Message from producer\n")
	s.mu.RLock()
	topic, ok := s.topics[in.topicName]
	part_raft := s.parts_rafts
	s.mu.RUnlock()

	if !ok {
		ret = "this topic is not in this broker"
		logger.DEBUG(logger.DError, "Topic %v,is not in this broker", in.topicName)
		return ret, errors.New(ret)
	}

	switch in.ack {
	case -1: //raft同步，并写入
		ret, err = part_raft.Append(in)
	case 1: //leader写入，不等待同步
		err = topic.addMessage(in)
	case 0: //直接返回
		go func() {
			err := topic.addMessage(in)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
		}()
	}

	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err.Error(), err
	}

	return ret, err
}

func (s *Server) InfoHandle(ipport string) error {
	logger.DEBUG(logger.DLog, "get consumer's ip_port %v\n", ipport)
	client, err := client_operations.NewClient("client", client.WithHostPorts(ipport))
	if err == nil {
		logger.DEBUG(logger.DLog, "connect consumer server successful\n")
		s.mu.Lock()
		consumer, ok := s.consumers[ipport]
		if !ok {
			consumer = NewClient(ipport, client)
			s.consumers[ipport] = consumer
		}
		go s.CheckConsumer(consumer)
		s.mu.Unlock()
		logger.DEBUG(logger.DLog, "return resp to consumer\n")
		return nil
	}

	logger.DEBUG(logger.DError, "connect client failed\n")
	return err
}

func (s *Server) StartGet(in info) (err error) {
	//新开启一个consumer关于一个topic和partition的协程来消费该partition的信息：
	//查询是否有该订阅的消息
	// PTP:需要负载均衡
	// PSB:不需要负载均衡，每个PSB开一个Part来发送消息
	err = nil
	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.mu.RLock()
		defer s.mu.RUnlock()
		//已经由zkserver检查过是否订阅
		sub_name := GetStringfromSub(in.topicName, in.partName, in.option)
		//添加到Config后会进行负载均衡，生成新的配置，然后执行新的配置
		return s.topics[in.topicName].HandleStartToGet(sub_name, in, s.consumers[in.consumer].GetCli())
	case TOPIC_KEY_PSB_PUSH:
		s.mu.RLock()
		defer s.mu.RUnlock()
		sub_name := GetStringfromSub(in.topicName, in.partName, in.option)
		logger.DEBUG(logger.DLog, "consumer(%v) start to get topic(%v) partition(%v) offset(%v) in sub(%v)\n", in.consumer, in.topicName, in.partName, in.offset, sub_name)
		return s.topics[in.topicName].HandleStartToGet(sub_name, in, s.consumers[in.consumer].GetCli())
	default:
		err = errors.New("the option is not PTP or PSB")
	}
	return err
}

func (s *Server) CheckConsumer(client *Client) {
	shutdouwn := client.CheckConsumer()
	if shutdouwn { //该consumer已关闭，平衡subscription
		client.mu.Lock()
		for _, subscription := range client.subList {
			subscription.ShutdownConsumerInGroup(client.name)
		}
		client.mu.Unlock()
	}
}

// PullHandle
// Pull message
func (s *Server) PullHandle(in info) (MSGS, error) {
	//若该请求属于PTP则读取index，获取上次的index，写入zookeeper中
	logger.DEBUG(logger.DLog, "%v get pull request the in.op(%v) TOP_PTP_PULL(%v)\n", s.Name, in.option, TOPIC_NIL_PTP_PULL)
	if in.option == TOPIC_NIL_PTP_PULL {
		s.zkclient.UpdatePTPOffset(context.Background(), &api.UpdatePTPOffsetRequest{
			Topic:  in.topicName,
			Part:   in.partName,
			Offset: in.offset,
		})
	}

	s.mu.RLock()
	topic, ok := s.topics[in.topicName]
	s.mu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DError, "this topic is not in this broker")
		return MSGS{}, errors.New("this topic is not in this broker")
	}

	return topic.PullMessage(in)
}

// 主动向leader pull信息，当获取信息失败后将询问zkserver，新的leader
func (s *Server) FetchMsg(in info, cli *server_operations.Client, topic *Topic) (ret string, err error) {
	//向zkserver请求向leader Broker Pull信息

	//向LeaderBroker发起Pull请求
	//获得本地当前文件end_index
	File, fd := topic.GetFile(in)
	index := File.GetIndex(fd)
	index += 1
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err.Error(), err
	}

	logger.DEBUG(logger.DLog2, "the cli is %v\n", cli)

	if in.fileName != "NowBlock.txt" {
		//当文件名不为nowblock时（历史块同步）
		//创建一partition，并向该File中写入内容

		go func() {
			Partition := NewPartition(s.Name, in.topicName, in.partName)
			Partition.StartGetMessage(File, fd, in)
			ice := 0

			for {
				resp, err := (*cli).Pull(context.Background(), &api.PullRequest{
					Consumer: s.Name,
					Topic:    in.topicName,
					Key:      in.partName,
					Offset:   index,
					Size:     10,
					Option:   TOPIC_KEY_PSB_PULL,
				})
				num := len(in.fileName)
				if err != nil {
					ice++
					logger.DEBUG(logger.DError, "Err %v,err(%v)\n", resp, err.Error())

					if ice >= 3 {
						time.Sleep(time.Second * 3)
						//询问新的Leader
						resp, err := s.zkclient.GetNewLeader(context.Background(), &api.GetNewLeaderRequest{
							TopicName: in.topicName,
							PartName:  in.partName,
							BlockName: in.fileName[:num-4],
						})
						if err != nil {
							logger.DEBUG(logger.DError, "%v\n", err.Error())
						}
						s.mu.Lock()
						_, ok := s.brokers_fetch[in.topicName+in.partName]
						if !ok {
							logger.DEBUG(logger.DLog, "this broker (%v) is not connected\n", s.Name)
							leader_bro, err := server_operations.NewClient(s.Name, client.WithHostPorts(resp.HostPort))
							if err != nil {
								logger.DEBUG(logger.DError, "%v\n", err.Error())
								s.mu.Unlock()
								return
							}
							s.brokers_fetch[resp.LeaderBroker] = &leader_bro
							cli = &leader_bro
						}
						s.mu.Unlock()
					}
					continue
				}
				if resp.Err == "file EOF" {
					logger.DEBUG(logger.DLog, "This partition(%d) filename(%d) is over\n", in.partName, in.fileName)
					fd.Close()
					return
				}
				ice = 0

				if resp.StartIndex <= index && resp.EndIndex > index {
					//index处于返回包的中间位置
					//需要截断该包，并写入
					//your code
					logger.DEBUG(logger.DLog, "need your code\n")
				}
				node := Key{
					Start_index: resp.StartIndex,
					End_index:   resp.EndIndex,
					Size:        int64(len(resp.Msgs)),
				}

				File.WriteFile(fd, node, resp.Msgs)
				index = resp.EndIndex + 1
				s.zkclient.UpdateRep(context.Background(), &api.UpdateRepRequest{
					Topic:      in.topicName,
					Part:       in.partName,
					BrokerName: s.Name,
					BlockName:  GetBlockName(in.fileName),
					EndIndex:   resp.EndIndex,
				})
			}
		}()
	} else {
		//当文件名为nowblock时（实时同步）
		//zkserver已经让该broker准备接收文件
		//直接调用addMessage

		go func() {
			fd.Close()
			s.mu.RLock()
			topic, ok := s.topics[in.topicName]
			s.mu.RUnlock()
			if !ok {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
			ice := 0
			for {
				// 检测fetch机制是否还在运行
				s.mu.RLock()
				_, ok := s.parts_fetch[in.topicName+in.partName+in.fileName]
				s.mu.RUnlock()
				if !ok {
					logger.DEBUG(logger.DLog, "this topic(%v)-partition(%v) is not in this broker\n", in.topicName, in.partName)
					return
				}

				resp, err := (*cli).Pull(context.Background(), &api.PullRequest{
					Consumer: s.Name,
					Topic:    in.topicName,
					Key:      in.partName,
					Offset:   index,
					Size:     10,
					Option:   TOPIC_KEY_PSB_PULL,
				})

				if err != nil {
					ice++
					respErr := ""
					if resp != nil {
						respErr = resp.Err
					}
					logger.DEBUG(logger.DError, "Err %v,err(%v)\n", respErr, err.Error())

					if ice >= 3 {
						resp, err := s.zkclient.GetNewLeader(context.Background(), &api.GetNewLeaderRequest{
							TopicName: in.topicName,
							PartName:  in.partName,
							BlockName: "NowBlock",
						})
						if err != nil {
							logger.DEBUG(logger.DError, "%v\n", err.Error())
							time.Sleep(time.Second * 3)
							continue
						}
						if resp == nil || resp.HostPort == "" {
							logger.DEBUG(logger.DError, "get new leader returned empty response\n")
							time.Sleep(time.Second * 3)
							continue
						}
						s.mu.Lock()
						_, ok := s.brokers_fetch[in.topicName+in.partName]
						if !ok {
							logger.DEBUG(logger.DLog, "this broker (%v) is not connected\n", s.Name)
							leader_bro, err := server_operations.NewClient(s.Name, client.WithHostPorts(resp.HostPort))
							if err != nil {
								logger.DEBUG(logger.DError, "%v\n", err.Error())
								s.mu.Unlock()
								return
							}
							s.brokers_fetch[resp.LeaderBroker] = &leader_bro
							cli = &leader_bro
						}
						s.mu.Unlock()
					}
					continue
				}
				ice = 0

				if resp.Size == 0 {
					//等待新消息的生产
					time.Sleep(time.Second * 10)
				} else {
					msgs := make([]Message, resp.Size)
					json.Unmarshal(resp.Msgs, &msgs)

					start_index := resp.StartIndex
					for _, msg := range msgs {
						if index == start_index {
							err := topic.addMessage(info{
								topicName:  in.topicName,
								partName:   in.partName,
								size:       msg.Size,
								message:    msg.Msg,
								BrokerName: s.Name,
								zkclient:   &s.zkclient,
								fileName:   "NowBlock.txt",
							})
							if err != nil {
								logger.DEBUG(logger.DError, "%v\n", err.Error())
							}
						}
						index++
					}
				}
			}
		}()
	}
	return ret, err
}
