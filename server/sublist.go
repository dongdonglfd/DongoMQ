package server

import (
	"DongoMQ/kitex_gen/api"
	"DongoMQ/kitex_gen/api/client_operations"
	"DongoMQ/kitex_gen/api/zkserver_operations"
	"DongoMQ/logger"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"sync"
)

const (
	TOPIC_NIL_PTP_PUSH = int8(1) //PTP---Push
	TOPIC_NIL_PTP_PULL = int8(2)
	TOPIC_KEY_PSB_PUSH = int8(3) //map[cli_name]offset in a position
	TOPIC_KEY_PSB_PULL = int8(4)

	TOPIC_NIL_PSB = 10

	VIRTUAL_1  = 1
	VIRTUAL_10 = 10
	VIRTUAL_20 = 20
	VIRTUAL_30 = 30
	VIRTUAL_40 = 40
	VIRTUAL_50 = 50

	OFFSET = 0
)

type Topic struct {
	mu      sync.RWMutex
	Broker  string
	Name    string
	Files   map[string]*File
	Parts   map[string]*Partition
	subList map[string]*SubScription
}

func NewTopic(broker_name, topic_name string) *Topic {
	topic := &Topic{
		mu:      sync.RWMutex{},
		Broker:  broker_name,
		Name:    topic_name,
		Parts:   make(map[string]*Partition),
		subList: make(map[string]*SubScription),
		Files:   make(map[string]*File),
	}
	str, _ := os.Getwd()
	str += "/" + broker_name + "/" + topic_name
	err := CreateList(str) //则存在，则不会创建
	if err != nil {
		logger.DEBUG(logger.DError, "create list failed %v\n", err.Error())
		return nil
	}

	return topic
}

func (t *Topic) PrepareAcceptHandle(in info) (ret string, err error) {
	t.mu.Lock()
	partition, ok := t.Parts[in.partName]
	if !ok {
		partition = NewPartition(t.Broker, t.Name, in.partName)
		t.Parts[in.partName] = partition
	}

	//设置partition中的file和fd,start_index等信息
	str, _ := os.Getwd()
	str += "/" + t.Broker + "/" + in.topicName + "/" + in.partName + "/" + in.fileName
	file, fd, Err, err := NewFile(str)
	if err != nil {
		t.mu.Unlock()
		return Err, err
	}
	t.Files[str] = file
	t.mu.Unlock()
	ret = partition.StartGetMessage(file, fd, in)
	if ret == OK {
		logger.DEBUG(logger.DLog, "topic(%v)_partition(%v) Start success\n", in.topicName, in.partName)
	} else {
		logger.DEBUG(logger.DLog, "topic(%v)_partition(%v) had started\n", in.topicName, in.partName)
	}
	return ret, nil
}

func (t *Topic) CloseAcceptPart(in info) (start, end int64, ret string, err error) {
	t.mu.Lock()
	partition, ok := t.Parts[in.partName]
	t.mu.Unlock()
	if !ok {
		ret = "this partition is not in the broker"
		logger.DEBUG(logger.DError, "this partition (%v) is not in this broker\n", in.partName)
		return 0, 0, ret, errors.New(ret)
	}
	start, end, ret, err = partition.CloseAcceptMessage(in)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	} else {
		str, _ := os.Getwd()
		str += "/" + t.Broker + "/" + in.topicName + "/" + in.partName + "/"
		t.mu.Lock()
		t.Files[str+in.newName] = t.Files[str+in.fileName]
		delete(t.Files, str+in.fileName)
		t.mu.Unlock()
	}
	return start, end, ret, err
}

func (t *Topic) addMessage(in info) error {
	t.mu.RLock()
	part, ok := t.Parts[in.partName]
	t.mu.RUnlock()

	if !ok {
		logger.DEBUG(logger.DError, "not find this part in add message\n")
		part := NewPartition(t.Broker, in.topicName, in.partName) //new a Partition //需要向sub中和config中加入一个partition
		t.mu.Lock()
		t.Parts[in.partName] = part
		t.mu.Unlock()
	}

	logger.DEBUG(logger.DLog, "topic(%v) use partition(%v) addMessage\n", t.Name, in.partName)
	part.AddMessage(in)

	return nil
}

func (t *Topic) PrepareSendHandle(in info, zkclient *zkserver_operations.Client) (ret string, err error) {
	sub_name := GetStringfromSub(in.topicName, in.partName, in.option)

	t.mu.Lock()
	//检查或创建partition
	partition, ok := t.Parts[in.partName]
	if !ok {
		partition = NewPartition(t.Broker, t.Name, in.partName)
		t.Parts[in.partName] = partition
	}

	//检查文件是否存在，若存在为获得File则创建File，若没有则返回错误
	str, _ := os.Getwd()
	str += "/" + t.Broker + "/" + in.topicName + "/" + in.partName + "/" + in.fileName
	file, ok := t.Files[str]
	if !ok {
		file_ptr, fd, Err, err := CheckFile(str)
		if err != nil {
			t.mu.Unlock()
			return Err, err
		}
		fd.Close()
		file = file_ptr
		t.Files[str] = file
	}

	//检查或创建sub
	sub, ok := t.subList[sub_name]
	if !ok {
		logger.DEBUG(logger.DLog, "%v create a new sub(%v)\n", t.Broker, sub_name)
		sub = NewSubScription(in, sub_name, t.Parts, t.Files)
		t.subList[sub_name] = sub
	}
	t.mu.Unlock()

	//在sub中创建对应文件的config，来等待startget
	if in.option == TOPIC_NIL_PTP_PUSH {
		ret, err = sub.AddPTPConfig(in, partition, file, zkclient)
	} else if in.option == TOPIC_KEY_PSB_PUSH {
		sub.AddPSBConfig(in, in.partName, file, zkclient)
	} else if in.option == TOPIC_NIL_PTP_PULL || in.option == TOPIC_KEY_PSB_PULL {
		//在sub中创建一个Node用来保存该consumer的Pull的文件描述符等信息
		logger.DEBUG(logger.DLog, "the file is %v\n", file)
		sub.AddNode(in, file)
	}

	return ret, err
}

// topic + "nil" + "ptp" (point to point consumer : partition为 1 : n)
// topic + key + "psb" (pub and sub consumer : partition 为 n : 1)
// topic + "nil" + "psb" (pub and sub consumer : partition 为 n : n)
func GetStringfromSub(topicName, partName string, option int8) string {
	ret := topicName
	if option == TOPIC_NIL_PTP_PUSH || option == TOPIC_NIL_PTP_PULL { //point to point
		ret = ret + "nil" + "ptp"
	} else if option == TOPIC_KEY_PSB_PUSH || option == TOPIC_KEY_PSB_PULL { //pub and sub
		ret = ret + partName + "psb"
	}
	return ret
}

func (t *Topic) HandleStartToGet(sub_name string, in info, cli *client_operations.Client) (err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	sub, ok := t.subList[sub_name]
	if !ok {
		ret := "this topic not have this subscription"
		logger.DEBUG(logger.DLog, "%v\n", ret)
		return errors.New(ret)
	}
	sub.AddConsumerInConfig(in, cli)
	return nil
}

func (t *Topic) GetFile(in info) (File *File, Fd *os.File) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	str, _ := os.Getwd()
	str += "/" + t.Broker + "/" + in.topicName + "/" + in.partName + "/" + in.fileName
	file, ok := t.Files[str]
	if !ok {
		file, fd, Err, err := NewFile(str)
		if err != nil {
			logger.DEBUG(logger.DError, "Err(%v),err(%v)\n", Err, err.Error())
			return nil, nil
		}
		Fd = fd
		t.Files[str] = file
	} else {
		Fd = file.OpenFileRead()
	}
	return file, Fd
}

func (t *Topic) PullMessage(in info) (MSGS, error) {
	logger.DEBUG(logger.DLog, "the info %v\n", in)
	sub_name := GetStringfromSub(in.topicName, in.partName, in.option)
	t.mu.RLock()
	sub, ok := t.subList[sub_name]
	t.mu.RUnlock()
	if !ok {
		str := fmt.Sprintf("%v this topic(%v) don't have sub(%v) the sublist is %v for %v\n", t.Broker, t.Name, sub_name, t.subList, in.consumer)
		logger.DEBUG(logger.DError, "%v\n", str)
		return MSGS{}, errors.New(str)
	}

	return sub.PullMsgs(in)
}

func (t *Topic) AddSubscription(in info) (retsub *SubScription, err error) {
	ret := GetStringfromSub(in.topicName, in.partName, in.option)
	t.mu.RLock()
	sub, ok := t.subList[ret]
	t.mu.RUnlock()

	if !ok {
		t.mu.Lock()
		subscription := NewSubScription(in, ret, t.Parts, t.Files)
		t.subList[ret] = subscription
		sub = subscription
		t.mu.Unlock()
	} else {
		sub.AddConsumerInGroup(in)
	}

	return sub, nil
}

func (t *Topic) ReduceSubscription(in info) (string, error) {
	ret := GetStringfromSub(in.topicName, in.partName, in.option)
	t.mu.Lock()
	defer t.mu.Unlock()
	sub, ok := t.subList[ret]
	if !ok {
		return ret, errors.New("this topic do not have this subscription")
	} else {
		sub.ReduceConsumer(in)
	}
	return ret, nil
}

const (
	START = "start"
	CLOSE = "close"
)

type Partition struct {
	mu     sync.RWMutex
	Broker string
	key    string
	state  string

	file_name   string
	file        *File
	fd          *os.File
	index       int64
	start_index int64
	queue       []Message
}

func NewPartition(broker_name, topic_name, part_name string) *Partition {
	part := &Partition{
		mu:     sync.RWMutex{},
		Broker: broker_name,
		state:  CLOSE,
		key:    part_name,
		index:  0,
	}

	str, _ := os.Getwd()
	str += "/" + broker_name + "/" + topic_name + "/" + part_name
	CreateList(str) //若存在，则不会创建

	return part
}

const (
	ErrHadStart = "this partition had start"
)

func (p *Partition) StartGetMessage(file *File, fd *os.File, in info) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	ret := ""
	switch p.state {
	case ALIVE:
		ret = ErrHadStart
	case CLOSE:
		p.state = ALIVE
		p.file = file
		p.fd = fd
		p.file_name = in.fileName
		p.index = file.GetIndex(fd)
		p.start_index = p.index
		ret = OK
	}
	return ret
}

func (p *Partition) CloseAcceptMessage(in info) (start, end int64, ret string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == ALIVE {
		str, _ := os.Getwd()
		str += "/" + p.Broker + "/" + in.topicName + "/" + in.partName
		err = p.file.Update(str, in.newName) //修改本地文件名
		p.file_name = in.newName

		// 强制刷新缓存中的剩余消息，写入磁盘，防止丢失
		if len(p.queue) > 0 {
			p.flushToDisk()
		}

		p.state = DOWN
		end = p.index
		start = p.file.GetFirstIndex(p.fd)
		p.fd.Close()
	} else if p.state == DOWN {
		ret = "this partition had close"
		logger.DEBUG(logger.DLog, "%v\n", ret)
		err = errors.New(ret)
	}
	return start, end, ret, err
}

func (p *Partition) GetFile() *File {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file
}

func (p *Partition) AddMessage(in info) (ret string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == DOWN {
		ret := "this partition had close"
		logger.DEBUG(logger.DLog, "%v\n", ret)
		return ret, errors.New(ret)
	}
	p.index++
	msg := Message{
		Index:      p.index,
		Size:       in.size,
		Topic_name: in.topicName,
		Part_name:  in.partName,
		Msg:        in.message,
	}
	logger.DEBUG(logger.DLog, "part_name(%v) add message %v index is %v size is %v\n", p.key, msg, p.index, p.index-p.start_index)

	p.queue = append(p.queue, msg) //加入队列

	// pull/fetch 读取磁盘文件；小批量生产后也要立即可见。
	p.flushToDisk()

	if in.zkclient != nil {
		(*in.zkclient).UpdateRep(context.Background(), &api.UpdateRepRequest{
			Topic:      in.topicName,
			Part:       in.partName,
			BrokerName: in.BrokerName,
			BlockName:  GetBlockName(in.fileName),
			EndIndex:   p.index,
		})
	}

	return ret, err
}

func (p *Partition) flushToDisk() {
	if len(p.queue) == 0 {
		return
	}

	data_msg, err := json.Marshal(p.queue)
	if err != nil {
		logger.DEBUG(logger.DError, "marshal partition queue failed: %v\n", err.Error())
		return
	}

	// 写入所有缓存消息
	node := Key{
		Start_index: p.queue[0].Index,
		End_index:   p.queue[len(p.queue)-1].Index,
		Size:        int64(len(data_msg)),
	}

	if !p.file.WriteFile(p.fd, node, data_msg) {
		return
	}

	// 清空缓存
	p.start_index = p.index + 1
	p.queue = p.queue[:0]
}

type SubScription struct {
	mu         sync.RWMutex
	name       string
	topic_name string

	option int8 // PTP / PSB

	groups []*Group

	partitions map[string]*Partition
	Files      map[string]*File

	//需要修改，一个订阅需要多个config，因为一个partition有多个文件，一个文件需要一个config
	//需要修改，分为多种订阅，每种订阅方式一种config
	PTP_config *Config

	//partition_name + consumer_name to config
	PSB_configs map[string]*PSBConfig_PUSH

	//一个consumer向文件描述符等的映射，每次pull将使用上次的文件描述符等资源
	//topic+partition+consumer to Node
	nodes map[string]*Node
}

func NewSubScription(in info, name string, parts map[string]*Partition, files map[string]*File) *SubScription {
	sub := &SubScription{
		mu:          sync.RWMutex{},
		name:        name,
		topic_name:  in.topicName,
		partitions:  parts,
		Files:       files,
		PTP_config:  nil,
		PSB_configs: make(map[string]*PSBConfig_PUSH),
		nodes:       make(map[string]*Node),
	}

	group := NewGroup(in.partName, in.consumer)
	sub.groups = append(sub.groups, group)
	return sub
}

// 当有消费者需要开始消费时，PTP
// 若sub中该文件的config存在，则加入该config
// 若sub中该文件的config不存在，则创建一个config，并加入
func (s *SubScription) AddPTPConfig(in info, partition *Partition, file *File, zkclient *zkserver_operations.Client) (ret string, err error) {
	s.mu.RLock()
	if s.PTP_config == nil {
		s.PTP_config = NewConfig(in.topicName, 0, nil, nil)
	}

	err = s.PTP_config.AddPartition(in, partition, file, zkclient)
	s.mu.RUnlock()
	if err != nil {
		return err.Error(), err
	}
	return ret, nil
}

func (s *SubScription) AddPSBConfig(in info, partName string, file *File, zkclient *zkserver_operations.Client) {
	s.mu.RLock()
	_, ok := s.PSB_configs[partName+in.consumer]
	if !ok {
		config := NewPSBConfigPush(in, file, zkclient)
		s.PSB_configs[partName+in.consumer] = config
	} else {
		logger.DEBUG(logger.DLog, "This PSB has Start\n")
	}

	s.mu.RUnlock()
}

func (s *SubScription) AddNode(in info, file *File) {
	str := in.topicName + in.partName + in.consumer
	s.mu.Lock()
	_, ok := s.nodes[str]
	if !ok {
		node := NewNode(in, file)
		s.nodes[str] = node
	}
	s.mu.Unlock()
}

// 将group中添加consumer
func (s *SubScription) AddConsumerInGroup(in info) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.groups[0].AddClient(in.consumer)
	case TOPIC_KEY_PSB_PUSH:
		group := NewGroup(in.topicName, in.consumer)
		s.groups = append(s.groups, group)
	}
}

// 将config中添加consumer 当consumer StartGet时才调用
func (s *SubScription) AddConsumerInConfig(in info, cli *client_operations.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.PTP_config.AddCli(in.consumer, cli)
	case TOPIC_KEY_PSB_PUSH:
		config, ok := s.PSB_configs[in.partName+in.consumer]
		if !ok {
			logger.DEBUG(logger.DError, "this PSBConfig PUSH id not been\n")
		}
		config.Start(in, cli)
	}
}

func (s *SubScription) ShutdownConsumerInGroup(cliName string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.option {
	case TOPIC_NIL_PTP_PUSH: //point to point just one group
		s.groups[0].DownClient(cliName)
	case TOPIC_KEY_PSB_PUSH:
		for _, group := range s.groups {
			group.DownClient(cliName)
		}
	}

	return s.topic_name
}

// group和config中都需要减少，当有consumer取消订阅时调用
func (s *SubScription) ReduceConsumer(in info) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch in.option {
	case TOPIC_NIL_PTP_PUSH:
		s.groups[0].DeleteClient(in.consumer)
		s.PTP_config.DeleteCli(in.partName, in.consumer) //delete config中的consumer
	case TOPIC_KEY_PSB_PUSH:
		for _, group := range s.groups {
			group.DeleteClient(in.consumer)
		}
	}
}

func (s *SubScription) PullMsgs(in info) (MSGS, error) {
	node_name := in.topicName + in.partName + in.consumer
	s.mu.RLock()
	node, ok := s.nodes[node_name]
	s.mu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DError, "this sub has not have this node(%v)\n", node_name)
		return MSGS{}, errors.New("this sub has not have this node\n")
	}
	return node.ReadMSGS(in)
}

type Config struct {
	mu       sync.RWMutex
	part_num int //partition数
	con_num  int //consumer数

	part_close chan *Part

	PartToCon map[string][]string

	Partitions map[string]*Partition
	Files      map[string]*File
	Clis       map[string]*client_operations.Client

	parts map[string]*Part //partitionName to Part

	consistent *Consistent
}

func NewConfig(topicName string, partNum int, partitions map[string]*Partition, files map[string]*File) *Config {
	config := &Config{
		mu:       sync.RWMutex{},
		part_num: partNum,
		con_num:  0,

		part_close: make(chan *Part),

		PartToCon:  make(map[string][]string),
		Files:      files,
		Partitions: partitions,
		Clis:       make(map[string]*client_operations.Client),
		parts:      make(map[string]*Part),
		consistent: NewConsistent(),
	}

	go config.GetCloseChan(config.part_close)

	return config
}

func (c *Config) GetCloseChan(ch chan *Part) {
	for close := range ch {
		c.DeletePartition(close.partName, close.file)
	}
}

func (c *Config) DeleteCli(partName string, cliName string) {
	c.mu.Lock()

	c.con_num--
	delete(c.Clis, cliName)

	err := c.consistent.Reduce(cliName)
	logger.DEBUG(logger.DError, "%v\n", err.Error())

	c.mu.Unlock()

	c.RebalancePtoC()
	c.UpdateParts()

	for i, name := range c.PartToCon[partName] {
		if name == cliName {
			c.PartToCon[partName] = append(c.PartToCon[partName][:i], c.PartToCon[partName][i+1:]...)
			break
		}
	}
}

func (c *Config) AddPartition(in info, partition *Partition, file *File, zkclient *zkserver_operations.Client) error {
	c.mu.Lock()

	c.part_num++
	c.Partitions[in.partName] = partition
	c.Files[in.fileName] = file

	c.parts[in.partName] = NewPart(in, file, zkclient)
	c.parts[in.partName].Start(c.part_close)
	c.mu.Unlock()

	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
	return nil
}

// part消费完成，移除config中的Partition和Part
func (c *Config) DeletePartition(partName string, file *File) {
	c.mu.Lock()

	c.part_num--
	delete(c.parts, partName)
	delete(c.Files, partName)

	//该Part协程已经关闭，该partition的文件已经消费完毕
	c.mu.Unlock()

	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
}

// RebalancePtoC 负载均衡，将调整后的配置存入PartToCon
// 将Consistent中的ConH置false，循环两次Partitions
// 第一次拿取1个consumer
// 第二次拿取靠前的ConH为true的Consumer
// 直到遇到ConH为false的
func (c *Config) RebalancePtoC() {
	c.consistent.SetFreeNode() //将空闲节点设为len(consumers)
	c.consistent.TurnZero()    //将consumer全设为空闲

	parttocon := make(map[string][]string)

	c.mu.RLock()
	Parts := c.Partitions
	c.mu.RUnlock()

	for name := range Parts {
		node := c.consistent.GetNode(name)
		var array []string
		array, ok := parttocon[name]
		array = append(array, node)
		if !ok {
			parttocon[name] = array
		}
	}

	for {
		for name := range Parts {
			if c.consistent.GetFreeNodeNum() > 0 {
				node := c.consistent.GetNodeFree(name)
				var array []string
				array, ok := parttocon[name]
				array = append(array, node)
				if !ok {
					parttocon[name] = array
				}
			} else {
				break
			}
		}
		if c.consistent.GetFreeNodeNum() <= 0 {
			break
		}
	}
	c.mu.Lock()
	c.PartToCon = parttocon
	c.mu.Unlock()
}

// 根据PartToCon中的配置，更新Parts中的Clis
func (c *Config) UpdateParts() {
	c.mu.RLock()
	for partition_name, part := range c.parts {
		part.UpdateClis(c.PartToCon[partition_name], c.Clis)
	}
	c.mu.RUnlock()
}

// 向Clis加入此consumer的句柄，重新负载均衡，并修改Parts中的clis数组
func (c *Config) AddCli(cli_name string, cli *client_operations.Client) {
	c.mu.Lock()

	c.con_num++
	c.Clis[cli_name] = cli

	err := c.consistent.Add(cli_name, 1)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	c.mu.Unlock()
	c.RebalancePtoC()
	c.UpdateParts()
}

// 带虚拟节点的一致性哈希
type Consistent struct {
	//排序的hash虚拟节点(环形)
	hashSortedNodes []uint32
	//虚拟节点(consumer)对应的实际节点
	circle map[uint32]string
	//已绑定的consumer为true
	nodes map[string]bool

	//consumer以负责一个Partition为true
	ConH     map[string]bool
	FreeNode int

	mu sync.RWMutex
	//虚拟节点个数
	virtualNodeCount int
}

func NewConsistent() *Consistent {
	con := &Consistent{
		hashSortedNodes:  make([]uint32, 2),
		circle:           make(map[uint32]string),
		nodes:            make(map[string]bool),
		ConH:             make(map[string]bool),
		FreeNode:         0,
		mu:               sync.RWMutex{},
		virtualNodeCount: VIRTUAL_10,
	}
	return con
}

func (c *Consistent) SetFreeNode() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.FreeNode = len(c.ConH)
}

func (c *Consistent) GetFreeNodeNum() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.FreeNode
}

func (c *Consistent) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// add consumer name as node
func (c *Consistent) Add(node string, power int) error {
	if node == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.nodes[node]; ok {
		return errors.New("node is already existed")
	}
	c.nodes[node] = true
	c.ConH[node] = false
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

func (c *Consistent) Reduce(node string) error {
	if node == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.nodes[node]; !ok {
		return errors.New("node already delete")
	}

	delete(c.nodes, node)
	delete(c.ConH, node)

	for i := 0; i < c.virtualNodeCount; i++ {
		virtualKey := c.hashKey(node + strconv.Itoa(i))
		delete(c.circle, virtualKey)
		for j := 0; j < len(c.hashSortedNodes); j++ {
			if c.hashSortedNodes[j] == virtualKey && j != len(c.hashSortedNodes)-1 {
				c.hashSortedNodes = append(c.hashSortedNodes[:j], c.hashSortedNodes[j+1:]...)
			} else if j == len(c.hashSortedNodes)-1 {
				c.hashSortedNodes = c.hashSortedNodes[:j]
			}
		}
	}

	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})

	return nil
}

func (c *Consistent) TurnZero() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.ConH {
		c.ConH[key] = false
	}
}

// return consumer name
func (c *Consistent) GetNode(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hash := c.hashKey(key)
	i := c.getPosition(hash)

	con := c.circle[c.hashSortedNodes[i]]

	c.ConH[con] = true
	c.FreeNode--

	return con
}

func (c *Consistent) GetNodeFree(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := c.hashKey(key)
	i := c.getPosition(hash)

	i += 1
	for {
		if i == len(c.hashSortedNodes)-1 {
			i = 0
		}
		con := c.circle[c.hashSortedNodes[i]]
		if !c.ConH[con] {
			c.ConH[con] = true
			c.FreeNode--
			return con
		}
		i++
	}
}

func (c *Consistent) getPosition(hash uint32) int {
	i := sort.Search(len(c.hashSortedNodes), func(i int) bool { return c.hashSortedNodes[i] >= hash })

	if i < len(c.hashSortedNodes) {
		if i == len(c.hashSortedNodes)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.hashSortedNodes) - 1
	}
}

type PSBConfig_PUSH struct {
	mu sync.RWMutex

	part_close chan *Part
	file       *File

	Cli  *client_operations.Client
	part *Part
}

func NewPSBConfigPush(in info, file *File, zkclient *zkserver_operations.Client) *PSBConfig_PUSH {
	ret := &PSBConfig_PUSH{
		mu:         sync.RWMutex{},
		part_close: make(chan *Part),
		file:       file,
		part:       NewPart(in, file, zkclient),
	}

	return ret
}

func (pc *PSBConfig_PUSH) Start(in info, cli *client_operations.Client) {
	pc.mu.Lock()
	pc.Cli = cli
	var names []string
	clis := make(map[string]*client_operations.Client)
	names = append(names, in.consumer)
	clis[in.consumer] = cli
	pc.part.UpdateClis(names, clis)
	pc.part.Start(pc.part_close)
	pc.mu.Unlock()
}
