package server

import (
	"DongoMQ/kitex_gen/api"
	"DongoMQ/kitex_gen/api/server_operations"
	"DongoMQ/kitex_gen/api/zkserver_operations"
	"DongoMQ/logger"
	"DongoMQ/zookeeper"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/cloudwego/kitex/server"
)

// RPCServer 可以注册两种server
// 一个是客户端（生产者和消费者连接的模式，负责订阅等请求和对zookeeper的请求）使用的server
// 另一个是Broker获取zookeeper信息和调度各个Broker的调度者
type RPCServer struct {
	srv_cli  *server.Server
	srv_bro  *server.Server
	zkinfo   zookeeper.ZkInfo
	server   *Server
	zkserver *ZKServer
}

func NewRPCServer(zkinfo zookeeper.ZkInfo) RPCServer {
	logger.LOGinit()
	return RPCServer{
		zkinfo: zkinfo,
	}
}

func (s *RPCServer) Start(opts_cli, opts_zks, opts_raf []server.Option, opt Options) error {
	switch opt.Tag {
	case BROKER:
		s.server = NewServer(s.zkinfo)
		s.server.Make(opt, opts_raf)

		srv_cli_bro := server_operations.NewServer(s, opts_cli...)
		s.srv_cli = &srv_cli_bro
		logger.DEBUG(logger.DLog, "%v the raft %v start rpcserver for clients\n", opt.Name, opt.Me)
		go func() {
			err := srv_cli_bro.Run()

			if err != nil {
				fmt.Println(err.Error())
			}
		}()

	case ZKBROKER:
		s.zkserver = NewZKServer(s.zkinfo)
		s.zkserver.make(opt)

		srv_bro_cli := zkserver_operations.NewServer(s, opts_zks...)
		s.srv_bro = &srv_bro_cli
		logger.DEBUG(logger.DLog, "ZkServer start rpcserver for brokers\n")
		go func() {
			err := srv_bro_cli.Run()

			if err != nil {
				logger.DEBUG(logger.DError, "start zkbroker failed"+err.Error())
			}
		}()
	}

	return nil
}

func (s *RPCServer) ShutDown_server() {
	if s.srv_bro != nil {
		(*s.srv_bro).Stop()
	}
	if s.srv_cli != nil {
		(*s.srv_cli).Stop()
	}
}

// producer--->zkserver
// 先在zookeeper上创建一个Topic,当生产该信息时，或消费信息时再有zkserver发送信息到broker让broker创建
func (s *RPCServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (r *api.CreateTopicResponse, err error) {
	info := s.zkserver.CreateTopic(Info_in{
		topicName: req.TopicName,
	})

	if info.Err != nil {
		return &api.CreateTopicResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.CreateTopicResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// producer--->zkserver
// 先在zookeeper上创建一个Partition,当生产该信息时，或消费信息时再有zkserver发送信息到broker让broker创建
func (s *RPCServer) CreatePart(ctx context.Context, req *api.CreatePartRequest) (r *api.CreatePartResponse, err error) {
	info := s.zkserver.CreatePart(Info_in{
		topicName: req.TopicName,
		partName:  req.PartName,
	})

	if info.Err != nil {
		return &api.CreatePartResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.CreatePartResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

func (s *RPCServer) SetPartitionState(ctx context.Context, req *api.SetPartitionStateRequest) (r *api.SetPartitionStateResponse, err error) {
	info := s.zkserver.SetPartitionState(Info_in{
		topicName: req.Topic,
		partName:  req.Part,
		option:    req.Option,
		repNum:    req.RepNum,
	})

	if info.Err != nil {
		return &api.SetPartitionStateResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.SetPartitionStateResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// broker---->zkserver
func (s *RPCServer) UpdateRep(ctx context.Context, req *api.UpdateRepRequest) (r *api.UpdateRepResponse, err error) {
	err = s.zkserver.UpdateRepNode(Info_in{
		topicName: req.Topic,
		partName:  req.Part,
		cliName:   req.BrokerName,
		blockName: req.BlockName,
		index:     req.EndIndex,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return &api.UpdateRepResponse{
			Ret: false,
		}, err
	}
	return &api.UpdateRepResponse{
		Ret: true,
	}, nil
}

// producer---->zkserver 获取该向哪个broker发送消息
func (s *RPCServer) ProGetLeader(ctx context.Context, req *api.ProGetLeaderRequest) (r *api.ProGetLeaderResponse, err error) {
	info := s.zkserver.ProGetLeader(Info_in{
		topicName: req.TopicName,
		partName:  req.PartName,
	})

	if info.Err != nil {
		return &api.ProGetLeaderResponse{
			Ret: false,
		}, info.Err
	}

	return &api.ProGetLeaderResponse{
		Ret:            true,
		BrokerHostPort: info.broHostPort,
	}, nil
}

// zkserver---->broker server
// 通知broker准备接收生产者信息
func (s *RPCServer) PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest) (r *api.PrepareAcceptResponse, err error) {
	ret, err := s.server.PrepareAcceptHandle(info{
		topicName: req.TopicName,
		partName:  req.PartName,
		fileName:  req.FileName,
	})
	if err != nil {
		return &api.PrepareAcceptResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.PrepareAcceptResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// zkserver---->broker server
// zkserver通知broker检查partition的state是否设置
// raft集群，或fetch机制是否开启
func (s *RPCServer) PrepareState(ctx context.Context, req *api.PrepareStateRequest) (r *api.PrepareStateResponse, err error) {
	var brokers BrokerS
	json.Unmarshal(req.Brokers, &brokers)
	ret, err := s.server.PrepareState(info{
		topicName: req.TopicName,
		partName:  req.PartName,
		option:    req.State,
		brokers:   brokers.BroBrokers,
	})
	if err != nil {
		return &api.PrepareStateResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.PrepareStateResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// producer---->broker server
func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (r *api.PushResponse, err error) {
	ret, err := s.server.PushHandle(info{
		producer:  req.Producer,
		topicName: req.Topic,
		partName:  req.Key,
		ack:       req.Ack,
		message:   req.Message,
	})

	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	return &api.PushResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// consumer---->broker server
// 获取broker的ip和port
func (s *RPCServer) ConInfo(ctx context.Context, req *api.InfoRequest) (r *api.InfoResponse, err error) {
	err = s.server.InfoHandle(req.IpPort)
	if err == nil {
		return &api.InfoResponse{
			Ret: true,
		}, nil
	}
	return &api.InfoResponse{Ret: false}, err
}

// consumer---->broker server
func (s *RPCServer) StartToGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetResponse, err error) {
	err = s.server.StartGet(info{
		consumer:  req.CliName,
		topicName: req.TopicName,
		partName:  req.PartName,
		offset:    req.Offset,
		option:    req.Option,
	})

	if err == nil {
		return &api.InfoGetResponse{
			Ret: true,
		}, nil
	}

	return &api.InfoGetResponse{Ret: false}, err
}

// consumer---->broker server
func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (r *api.PullResponse, err error) {
	Err := "ok"
	ret, err := s.server.PullHandle(info{
		consumer:  req.Consumer,
		topicName: req.Topic,
		partName:  req.Key,
		size:      req.Size,
		offset:    req.Offset,
		option:    req.Option,
	})
	if err != nil {
		if err == io.EOF && ret.size == 0 {
			Err = "file EOF"
		} else {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return &api.PullResponse{
				Ret: false,
				Err: err.Error(),
			}, err
		}
	}

	return &api.PullResponse{
		Msgs:       ret.array,
		StartIndex: ret.start_index,
		EndIndex:   ret.end_index,
		Size:       ret.size,
		Err:        Err,
	}, nil
}

func (s *RPCServer) UpdatePTPOffset(ctx context.Context, req *api.UpdatePTPOffsetRequest) (r *api.UpdatePTPOffsetResponse, err error) {
	err = s.zkserver.UpdatePTPOffset(Info_in{
		topicName: req.Topic,
		partName:  req.Part,
		index:     req.Offset,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return &api.UpdatePTPOffsetResponse{
			Ret: false,
		}, err
	}
	return &api.UpdatePTPOffsetResponse{
		Ret: false,
	}, nil
}

func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (r *api.SubResponse, err error) {
	err = s.zkserver.SubHandle(Info_in{
		cliName:   req.Consumer,
		topicName: req.Topic,
		partName:  req.Key,
		option:    req.Option,
	})

	if err == nil {
		return &api.SubResponse{
			Ret: true,
		}, nil
	}

	return &api.SubResponse{Ret: false}, err
}

func (s *RPCServer) ConStartGetBroker(ctx context.Context, req *api.ConStartGetBrokRequest) (r *api.ConStartGetBrokResponse, err error) {
	parts, size, err := s.zkserver.HandStartGetBroker(Info_in{
		cliName:   req.CliName,
		topicName: req.TopicName,
		partName:  req.PartName,
		option:    req.Option,
		index:     req.Index,
	})
	if err != nil {
		return &api.ConStartGetBrokResponse{
			Ret: false,
		}, err
	}

	return &api.ConStartGetBrokResponse{
		Ret:   true,
		Size:  int64(size),
		Parts: parts,
	}, nil
}

// zkserver---->broker server
// zkserver控制broker停止接收某个partition的信息
// 并修改文件名，关闭partition的fd等(指NowBlock的接收信息)
// 调用者需向zookeeper修改节点信息
func (s *RPCServer) CloseAccept(ctx context.Context, req *api.CloseAcceptRequest) (r *api.CloseAcceptResponse, err error) {
	start, end, ret, err := s.server.CloseAcceptHandle(info{
		topicName: req.TopicName,
		partName:  req.PartName,
		fileName:  req.Oldfilename,
		newName:   req.Newfilename_,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "Err %v err(%v)\n", ret, err.Error())
		return &api.CloseAcceptResponse{
			Ret: false,
		}, err
	}

	return &api.CloseAcceptResponse{
		Ret:        true,
		Startindex: start,
		Endindex:   end,
	}, nil
}

// zkserver---->broker server
// 通知broker准备向consumer发送信息
func (s *RPCServer) PrepareSend(ctx context.Context, req *api.PrepareSendRequest) (r *api.PrepareSendResponse, err error) {
	ret, err := s.server.PrepareSendHandle(info{
		consumer:  req.Consumer,
		topicName: req.TopicName,
		partName:  req.PartName,
		option:    req.Option,
		fileName:  req.FileName,
		offset:    req.Offset,
	})
	if err != nil {
		return &api.PrepareSendResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.PrepareSendResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// broker---->zkserver
// broker连接到zkserver后会立即发送info,让zkserver连接到broker
func (s *RPCServer) BroInfo(ctx context.Context, req *api.BroInfoRequest) (r *api.BroInfoResponse, err error) {
	err = s.zkserver.HandleBroInfo(req.BrokerName, req.BrokerHostPort)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return &api.BroInfoResponse{
			Ret: false,
		}, err
	}
	return &api.BroInfoResponse{
		Ret: true,
	}, nil
}

func (s *RPCServer) AddRaftPartition(ctx context.Context, req *api.AddRaftPartitionRequest) (r *api.AddRaftPartitionResponse, err error) {
	var brokers BrokerS
	json.Unmarshal(req.Brokers, &brokers)

	logger.DEBUG(logger.DLog, "the brokers in rpc is %v\n", req.Brokers)
	logger.DEBUG(logger.DLog, "Unmarshal brokers is %v\n", brokers.RafBrokers)

	ret, err := s.server.AddRaftHandle(info{
		topicName: req.TopicName,
		partName:  req.PartName,
		brokers:   brokers.RafBrokers,
		brok_me:   brokers.Me_Brokers,
	})
	if err != nil {
		return &api.AddRaftPartitionResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.AddRaftPartitionResponse{
		Ret: true,
		Err: ret,
	}, nil
}

func (s *RPCServer) CloseRaftPartition(ctx context.Context, req *api.CloseRaftPartitionRequest) (r *api.CloseRaftPartitionResponse, err error) {
	ret, err := s.server.CloseRaftHandle(info{
		topicName: req.TopicName,
		partName:  req.PartName,
	})
	if err != nil {
		return &api.CloseRaftPartitionResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.CloseRaftPartitionResponse{
		Ret: true,
		Err: ret,
	}, nil
}

func (s *RPCServer) AddFetchPartition(ctx context.Context, req *api.AddFetchPartitionRequest) (r *api.AddFetchPartitionResponse, err error) {
	var brokers BrokerS
	json.Unmarshal(req.Brokers, &brokers)

	ret, err := s.server.AddFetchHandle(info{
		topicName:    req.TopicName,
		partName:     req.PartName,
		LeaderBroker: req.LeaderBroker,
		HostPort:     req.HostPort,
		brokers:      brokers.BroBrokers,
		fileName:     req.FileName,
	})
	if err != nil {
		return &api.AddFetchPartitionResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.AddFetchPartitionResponse{
		Ret: true,
		Err: ret,
	}, nil
}

func (s *RPCServer) CloseFetchPartition(ctx context.Context, req *api.CloseFetchPartitionRequest) (r *api.CloseFetchPartitionResponse, err error) {
	ret, err := s.server.CloseFetchHandle(info{
		topicName: req.TopicName,
		partName:  req.PartName,
	})
	if err != nil {
		return &api.CloseFetchPartitionResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.CloseFetchPartitionResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// broker---->zkserver
func (s *RPCServer) BecomeLeader(ctx context.Context, req *api.BecomeLeaderRequest) (r *api.BecomeLeaderResponse, err error) {
	err = s.zkserver.BecomeLeader(Info_in{
		cliName:   req.Broker,
		topicName: req.Topic,
		partName:  req.Partition,
	})
	if err != nil {
		return &api.BecomeLeaderResponse{
			Ret: false,
		}, err
	} else {
		return &api.BecomeLeaderResponse{
			Ret: true,
		}, nil
	}
}

// consumer and producer ----> zkserver
func (s *RPCServer) GetNewLeader(ctx context.Context, req *api.GetNewLeaderRequest) (r *api.GetNewLeaderResponse, err error) {
	info, err := s.zkserver.GetNewLeader(Info_in{
		topicName: req.TopicName,
		partName:  req.PartName,
		blockName: req.BlockName,
	})

	if err != nil {
		return &api.GetNewLeaderResponse{
			Ret: false,
		}, err
	} else {
		return &api.GetNewLeaderResponse{
			Ret:          true,
			LeaderBroker: info.brokerName,
			HostPort:     info.broHostPort,
		}, nil
	}
}

func (s *RPCServer) Sub2(ctx context.Context, req *api.Sub2Request) (r *api.Sub2Response, err error) {
	err = s.server.SubHandle(info{
		consumer:  req.Consumer,
		topicName: req.Topic,
		partName:  req.Key,
		option:    req.Option,
	})

	if err != nil {
		return &api.Sub2Response{
			Ret: false,
		}, err
	} else {
		return &api.Sub2Response{
			Ret: true,
		}, err
	}
}
