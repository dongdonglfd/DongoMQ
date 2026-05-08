package server

import (
	"DongoMQ/kitex_gen/api/client_operations"
	"DongoMQ/kitex_gen/api/zkserver_operations"
	"DongoMQ/logger"
	"DongoMQ/zookeeper"
	"net"
	"os"
	"runtime"

	Ser "github.com/cloudwego/kitex/server"
)

type PartKey struct {
	Name string `json:"Name"`
}

// 初始化Broker时的消息
type Options struct {
	Me                 int
	Name               string
	Tag                string
	ZKServer_Host_Port string
	Broker_Host_Port   string
	Raft_Host_Port     string
}

// Broker向ZKServer发送自己的新能指标，用于按权值负载均衡
type Property struct {
	Name    string `json:"Name"`
	Power   int64  `json:"Power"`
	CPURate int64  `json:"CPURate"`
	DiskIO  int64  `json:"DiskIO"`
}

// Broker启动时获得的初始信息
type BroNodeInfo struct {
	Topics map[string]TopNodeInfo `json:"Topics"`
}

type TopNodeInfo struct {
	Topic_name string
	Part_nums  int
	Partitions map[string]ParNodeInfo
}

type ParNodeInfo struct {
	Part_name  string
	Block_nums int
	Blocks     map[string]BloNodeInfo
}

type BloNodeInfo struct {
	Start_index int64
	End_index   int64
	Path        string
	File_name   string
}

type BrokerS struct {
	BroBrokers map[string]string `json:"brobrokers"`
	RafBrokers map[string]string `json:"rafbrokers"`
	Me_Brokers map[string]int    `json:"me_brokers"`
}

const (
	ZKBROKER = "zkbroker"
	BROKER   = "broker"
)

func NewBrokerAndStart(zkinfo zookeeper.ZkInfo, opt Options) *RPCServer {
	//start the broker server
	addr_bro, _ := net.ResolveTCPAddr("tcp", opt.Broker_Host_Port)
	addr_raf, _ := net.ResolveTCPAddr("tcp", opt.Raft_Host_Port)
	var opts_bro, opts_raf []Ser.Option
	opts_bro = append(opts_bro, Ser.WithServiceAddr(addr_bro))
	opts_raf = append(opts_raf, Ser.WithServiceAddr(addr_raf))

	rpcServer := NewRPCServer(zkinfo)

	go func() {
		err := rpcServer.Start(opts_bro, nil, opts_raf, opt)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err)
		}
	}()

	return &rpcServer
}

func NewZKServerAndStart(zkinfo zookeeper.ZkInfo, opt Options) *RPCServer {
	//start the zookeeper server
	addr_zks, _ := net.ResolveTCPAddr("tcp", opt.ZKServer_Host_Port)
	var opts_zks []Ser.Option
	opts_zks = append(opts_zks, Ser.WithServiceAddr(addr_zks))

	rpcServer := NewRPCServer(zkinfo)

	go func() {
		err := rpcServer.Start(nil, opts_zks, nil, opt)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err)
		}
	}()

	return &rpcServer
}

func CheckFileOrList(path string) (ret bool) {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func MovName(OldFilePath, NewFilePath string) error {
	return os.Rename(OldFilePath, NewFilePath)
}

func CreateList(path string) error {
	ret := CheckFileOrList(path)

	if !ret {
		err := os.MkdirAll(path, 0775)
		if err != nil {
			_, file, line, _ := runtime.Caller(1)
			logger.DEBUG(logger.DError, "%v:%v mkdir %v error %v\n", file, line, path, err.Error())
			return err
		}
	}

	return nil
}

func CreateFile(path string) (file *os.File, err error) {
	return os.Create(path)
}

func GetBlockName(fileName string) (ret string) {
	ret = fileName[:len(fileName)-4]
	return ret
}

func CheckChangeCli(old map[string]*client_operations.Client, new []string) (reduce, add []string) {
	for _, new_cli := range new {
		if _, ok := old[new_cli]; !ok { //new_cli 在old中没有
			add = append(add, new_cli)
		}
	}

	for old_cli := range old {
		had := false //不存在
		for _, name := range new {
			if old_cli == name {
				had = true
				break
			}
		}
		if !had {
			reduce = append(reduce, old_cli)
		}
	}
	return reduce, add
}

// use for test
type Info struct {
	TopicName string
	PartName  string
	FileName  string
	NewName   string
	Option    int8
	Offset    int64
	Size      int8

	Ack int8

	Producer string
	Consumer string
	CmdIndex int64

	Message []byte

	//raft
	Brokers map[string]string
	Me      int

	//fetch
	LeaderBroker string
	HostPort     string

	//update rep
	Zkclient   *zkserver_operations.Client
	BrokerName string
}

// use in test
func GetInfo(in Info) info {
	return info{
		topicName:    in.TopicName,
		partName:     in.PartName,
		fileName:     in.FileName,
		newName:      in.NewName,
		option:       in.Option,
		offset:       in.Offset,
		size:         in.Size,
		ack:          in.Ack,
		producer:     in.Producer,
		consumer:     in.Consumer,
		cmdIndex:     in.CmdIndex,
		message:      in.Message,
		brokers:      in.Brokers,
		me:           in.Me,
		LeaderBroker: in.LeaderBroker,
		HostPort:     in.HostPort,
		zkclient:     in.Zkclient,
		BrokerName:   in.BrokerName,
	}
}

func GetServerInfoAply() chan info {
	return make(chan info)
}
