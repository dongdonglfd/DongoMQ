package clients

import (
	"DongoMQ/kitex_gen/api"
	ser "DongoMQ/kitex_gen/api/client_operations"
	"DongoMQ/kitex_gen/api/server_operations"
	"DongoMQ/kitex_gen/api/zkserver_operations"
	"DongoMQ/logger"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
)

type Consumer struct {
	mu    sync.RWMutex
	Name  string //唯一标识
	State string

	srv      server.Server
	port     string
	zkBroker zkserver_operations.Client
	Brokers  map[string]*server_operations.Client //broker_name--client
}

func NewConsumer(zkBroker string, name string, port string) (*Consumer, error) {
	c := Consumer{
		mu:      sync.RWMutex{},
		Name:    name,
		State:   "alive",
		port:    port,
		Brokers: make(map[string]*server_operations.Client),
	}
	var err error
	fmt.Println("the zkBroker address is ", zkBroker, "the port is ", port)
	c.zkBroker, err = zkserver_operations.NewClient(c.Name, client.WithHostPorts(zkBroker))

	return &c, err
}

func (c *Consumer) Alive() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.State
}

func (c *Consumer) Start_server() {
	addr, _ := net.ResolveTCPAddr("tcp", c.port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))

	svr := ser.NewServer(c, opts...)
	c.srv = svr
	err := svr.Run()
	if err != nil {
		fmt.Println(err.Error())
	}
}

func (c *Consumer) Shutdown_server() {
	c.srv.Stop()
}

func (c *Consumer) Down() {
	c.mu.Lock()
	c.State = "down"
	c.mu.Unlock()
}

func (c *Consumer) SendInfo(port string, cli *server_operations.Client) error {
	//consumer向broker发送自己的host和ip，使其连接上自己
	resp, err := (*cli).ConInfo(context.Background(), &api.InfoRequest{
		IpPort: port,
	})
	if err != nil {
		fmt.Println(resp)
	}
	return err
}

func (c *Consumer) Sub(topic, partition string, option int8) (err error) {
	//向server订阅topic和partition   option:订阅类型，PTP/PSB
	c.mu.RLock()
	zk := c.zkBroker
	c.mu.RUnlock()

	resp, err := zk.Sub(context.Background(), &api.SubRequest{
		Consumer: c.Name,
		Topic:    topic,
		Key:      partition,
		Option:   option,
	})

	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

type Info struct {
	Offset int64
	Topic  string
	Part   string
	Option int8
	Size   int8
	Cli    *server_operations.Client
	Bufs   map[int64]*api.PubRequest
}

func NewInfo(offset int64, topic, part string) Info {
	return Info{
		Offset: offset,
		Topic:  topic,
		Part:   part,
		Bufs:   make(map[int64]*api.PubRequest),
	}
}

func (c *Consumer) StartGet(info Info) (partkeys []PartKey, ret string, err error) {
	resp, err := c.zkBroker.ConStartGetBroker(context.Background(), &api.ConStartGetBrokRequest{
		CliName:   c.Name,
		TopicName: info.Topic,
		PartName:  info.Part,
		Option:    info.Option,
		Index:     info.Offset,
	})

	if err != nil || !resp.Ret {
		return nil, ret, err
	}

	var parts Parts
	err = json.Unmarshal(resp.Parts, &parts)
	if err != nil {
		return nil, "", err
	}
	logger.DEBUG(logger.DLog, "start get parts json is %v turn %v\n", resp.Parts, parts.PartKeys)
	if info.Option == 1 || info.Option == 3 { //pub
		ret, err = c.StartGetToBroker(parts.PartKeys, info)
	}
	return parts.PartKeys, ret, err
}

func (c *Consumer) StartGetToBroker(parts []PartKey, info Info) (ret string, err error) {
	//连接上各个broker，并发送start请求
	for _, part := range parts {
		if part.Err != "ok" {
			ret += part.Err
			continue
		}

		req := &api.InfoGetRequest{
			CliName:   c.Name,
			TopicName: info.Topic,
			PartName:  info.Part,
			Option:    info.Option,
			Offset:    part.Offset,
		}

		bro_cli, ok := c.Brokers[part.BrokerName]
		if !ok {
			bro_cli, err := server_operations.NewClient(c.Name, client.WithHostPorts(part.Broker_H_P))
			if err != nil {
				return ret, err
			}
			if info.Option == 1 { //ptp
				c.Brokers[part.BrokerName] = &bro_cli
				bro_cli.StartToGet(context.Background(), req)
			}
		}
		//发送info
		err = c.SendInfo(c.port, bro_cli)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return ret, err
		}

		if info.Option == 3 { //psb
			(*bro_cli).StartToGet(context.Background(), req)
		}

	}
	return ret, nil
}

func (c *Consumer) GetCli(part PartKey) (cli *server_operations.Client, err error) {
	cli, ok := c.Brokers[part.BrokerName]
	if !ok {
		bro_cli, err := server_operations.NewClient(c.Name, client.WithHostPorts(part.Broker_H_P))
		if err != nil {
			return nil, err
		}
		cli = &bro_cli
		c.Brokers[part.BrokerName] = cli
		logger.DEBUG(logger.DLog, "get cli(%v) fail,new client\n", part.BrokerName)
	}
	return cli, nil
}

type Msg struct {
	Index      int64  `json:"index"`
	Topic_name string `json:"topic_name"`
	Part_name  string `json:"part_name"`
	Msg        []byte `json:"msg"`
}

func (c *Consumer) Pull(info Info) (int64, int64, []Msg, error) {
	//向broker拉取信息
	resp, err := (*info.Cli).Pull(context.Background(), &api.PullRequest{
		Consumer: c.Name,
		Topic:    info.Topic,
		Key:      info.Part,
		Offset:   info.Offset,
		Size:     info.Size,
		Option:   info.Option,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return -1, -1, nil, err
	}

	start, end, msgs, err := decodePullResponse(resp)
	if err != nil {
		return start, end, nil, err
	}
	return start, end, msgs, nil
}

func decodePullResponse(resp *api.PullResponse) (int64, int64, []Msg, error) {
	if resp == nil {
		return -1, -1, nil, io.EOF
	}
	if resp.Err == "file EOF" || resp.Size == 0 || len(resp.Msgs) == 0 {
		return resp.StartIndex, resp.EndIndex, nil, io.EOF
	}

	var msgs []Msg
	err := json.Unmarshal(resp.Msgs, &msgs)
	if err != nil {
		return 0, 0, nil, err
	}

	return resp.StartIndex, resp.EndIndex, msgs, nil
}

func (c *Consumer) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	fmt.Println(req)

	/*
		添加用户自己的处理代码
	*/

	return &api.PubResponse{Ret: true}, nil
}

func (c *Consumer) Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	fmt.Println("ping pong")
	return &api.PingPongResponse{Pong: true}, nil
}
