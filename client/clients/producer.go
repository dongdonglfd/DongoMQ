package clients

import (
	"DongoMQ/kitex_gen/api"
	"DongoMQ/kitex_gen/api/server_operations"
	"DongoMQ/kitex_gen/api/zkserver_operations"
	"context"
	"errors"
	"sync"

	"github.com/cloudwego/kitex/client"
)

type Message struct {
	Topic     string //主题
	Partition string //分区
	Msg       []byte //信息
}

type Producer struct {
	mu sync.RWMutex

	Name            string //唯一标识
	ZkBroker        zkserver_operations.Client
	TopicPartitions map[string]server_operations.Client //map[topicname+partname]cli：缓存主题分区对应broker
	TopPartIndex    map[string]int64                    // 主题分区与偏移量
}

func NewProducer(zkBroker string, name string) (*Producer, error) {
	p := Producer{
		mu:              sync.RWMutex{},
		Name:            name,
		TopicPartitions: make(map[string]server_operations.Client),
	}
	var err error
	p.ZkBroker, err = zkserver_operations.NewClient(p.Name, client.WithHostPorts(zkBroker))

	return &p, err
}

func (p *Producer) CreateTopic(topicName string) error {
	resp, err := p.ZkBroker.CreateTopic(context.Background(), &api.CreateTopicRequest{
		TopicName: topicName,
	})
	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

func (p *Producer) CreatePart(topicName, partName string) error {
	resp, err := p.ZkBroker.CreatePart(context.Background(), &api.CreatePartRequest{
		TopicName: topicName,
		PartName:  partName,
	})

	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

func (p *Producer) SetPartitionState(topicName, partName string, option, repNum int8) error {
	resp, err := p.ZkBroker.SetPartitionState(context.Background(), &api.SetPartitionStateRequest{
		Topic:  topicName,
		Part:   partName,
		Option: option,
		RepNum: repNum,
	})

	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

func (p *Producer) Push(msg Message, ack int) error {
	str := msg.Topic + msg.Partition
	p.mu.RLock()
	cli, ok1 := p.TopicPartitions[str]
	zk := p.ZkBroker
	p.mu.RUnlock()

	if !ok1 {
		resp, err := zk.ProGetLeader(context.Background(), &api.ProGetLeaderRequest{
			TopicName: msg.Topic,
			PartName:  msg.Partition,
		})
		if err != nil || !resp.Ret {
			return err
		}

		cli, err = server_operations.NewClient(p.Name, client.WithHostPorts(resp.BrokerHostPort))

		if err != nil {
			return err
		}

		p.mu.Lock()
		p.TopicPartitions[str] = cli
		p.mu.Unlock()
	}

	//若partition所在的broker发生改变，将重新发送该信息，重新请求zkserver
	resp, err := cli.Push(context.Background(), &api.PushRequest{
		Producer: p.Name,
		Topic:    msg.Topic,
		Key:      msg.Partition,
		Message:  msg.Msg,
		Ack:      int8(ack),
	})
	if err == nil || resp.Ret {
		return nil
	} else if resp.Err == "partition remove" {
		p.mu.Lock()
		delete(p.TopicPartitions, str)
		p.mu.Unlock()

		return p.Push(msg, ack)
	} else {
		return errors.New("err == " + err.Error() + "or resp.Ret == false")
	}
}
