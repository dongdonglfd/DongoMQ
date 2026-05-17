package server

import (
	"DongoMQ/kitex_gen/api"
	"DongoMQ/kitex_gen/api/client_operations"
	"DongoMQ/kitex_gen/api/zkserver_operations"
	"DongoMQ/logger"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

const (
	ALIVE     = "alive"
	DOWN      = "down"
	TOUT      = 60 * 10
	UPDATANUM = 10
)

type Client struct {
	mu       sync.RWMutex
	name     string
	state    string
	consumer client_operations.Client

	subList map[string]*SubScription //若这个consumer关闭，则遍历这些订阅并修改
}

func NewClient(ipport string, con client_operations.Client) *Client {
	client := &Client{
		mu:       sync.RWMutex{},
		name:     ipport,
		consumer: con,
		state:    ALIVE,
		subList:  make(map[string]*SubScription),
	}
	return client
}

func (c *Client) CheckConsumer() bool {
	c.mu = sync.RWMutex{}

	for {
		resp, err := c.consumer.Pingpong(context.Background(), &api.PingPongRequest{
			Ping: true,
		})
		if err != nil || !resp.Pong {
			break
		}

		time.Sleep(time.Second)
	}

	c.mu.Lock()
	c.state = DOWN
	c.mu.Unlock()
	return true
}

func (c *Client) GetCli() *client_operations.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &c.consumer
}

func (c *Client) AddSubscription(sub *SubScription) {
	c.mu.Lock()
	c.subList[sub.name] = sub
	c.mu.Unlock()
}

func (c *Client) ReduceSubscription(name string) {
	c.mu.Lock()
	delete(c.subList, name)
	c.mu.Unlock()
}

type Part struct {
	mu        sync.RWMutex
	topicName string
	partName  string
	option    int8
	clis      map[string]*client_operations.Client
	zkclient  *zkserver_operations.Client

	state string
	fd    os.File
	file  *File

	index  int64 //use index to find offset
	offset int64

	start_index int64
	end_index   int64

	buffer_node map[int64]Key
	buffer_msg  map[int64][]Message

	part_had chan Done
	buf_done map[int64]string
}

const (
	OK     = "ok"
	TIOUT  = "timeout"
	NOTDO  = "notdo"
	HAVEDO = "havedo"
	HADDO  = "haddo"

	BUFF_NUM  = 5
	AGAIN_NUM = 3
)

type Done struct {
	in   int64
	err  string
	name string
	cli  *client_operations.Client
}

func NewPart(in info, file *File, zkclient *zkserver_operations.Client) *Part {
	part := &Part{
		mu:        sync.RWMutex{},
		topicName: in.topicName,
		partName:  in.partName,
		option:    in.option,
		zkclient:  zkclient,

		buffer_node: make(map[int64]Key),
		buffer_msg:  make(map[int64][]Message),
		file:        file,
		clis:        make(map[string]*client_operations.Client),
		state:       DOWN,

		part_had: make(chan Done),
		buf_done: make(map[int64]string),
	}

	part.index = in.offset

	return part
}

func (p *Part) Start(close chan *Part) {
	//open file
	p.fd = *p.file.OpenFileRead()
	offset, err := p.file.FindOffset(&p.fd, p.index)

	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	p.offset = offset
	for i := 0; i < BUFF_NUM; i++ { //加载BUFF_NUM个block到队列中
		err := p.AddBlock()
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
	}

	go p.GetDone(close)

	p.mu.Lock()
	if p.state == DOWN {
		p.state = ALIVE
	} else {
		p.mu.Unlock()
		logger.DEBUG(logger.DError, "the part is ALIVE in before this start\n")
		return
	}

	for name, cli := range p.clis {
		go p.SendOneBlock(name, cli)
	}
	p.mu.Unlock()
}

func (p *Part) UpdateClis(cli_names []string, Clis map[string]*client_operations.Client) {
	p.mu.Lock()
	reduce, add := CheckChangeCli(p.clis, cli_names)
	for _, name := range reduce {
		delete(p.clis, name)
		//删除一个consumer，在下一次发送时会检查
		//是否存在，则关闭这个consumer的循环发送
	}

	for _, name := range add {
		p.clis[name] = Clis[name]             //新加入一个负责该分片的consumer
		go p.SendOneBlock(name, p.clis[name]) //开启协程，发送消息
	}

	p.mu.Unlock()
}

func (p *Part) AddBlock() error {
	node, msg, err := p.file.ReadFile(&p.fd, p.offset)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.buf_done[node.Start_index] = NOTDO //新加入的块未被消费
	p.buffer_node[node.Start_index] = node
	p.buffer_msg[node.Start_index] = msg

	p.end_index = node.End_index
	p.offset += int64(NODE_SIZE) + int64(node.Size)

	p.mu.Unlock()
	return err
}

// 需要修改，设置未可主动关闭模式，使用管道
func (p *Part) GetDone(close chan *Part) {
	num := 0 //计数，如果达到UPDATENUM则更新zookeeper中的offset
	for {
		select {
		case do := <-p.part_had:
			if do.err == OK { //发送成功，buf_do--,buf_done++,补充buf_do
				num++
				err := p.AddBlock()
				p.mu.Lock()
				if err != nil {
					logger.DEBUG(logger.DError, "%v\n", err.Error())
				}

				//文件消费完成，且文件不是生产者正在写入的文件
				if p.file.filename != p.partName+"NowBlock.txt" && errors.Is(err, errors.New("read All file,do not find this index")) {
					p.state = DOWN
				}
				//且缓存中的文件页被消费完后，发送消息到config，关闭Part:
				if p.state == DOWN && len(p.buf_done) == 0 {
					p.mu.Unlock()
					close <- p
					return
				}

				p.buf_done[do.in] = HADDO
				in := p.start_index

				for {
					if p.buf_done[in] == HADDO {
						p.start_index = p.buffer_node[in].End_index + 1
						(*p.zkclient).UpdatePTPOffset(context.Background(), &api.UpdatePTPOffsetRequest{
							Topic:  p.topicName,
							Part:   p.partName,
							Offset: p.start_index,
						})
						delete(p.buf_done, in)
						delete(p.buffer_msg, in)
						delete(p.buffer_node, in)
						in = p.start_index
					} else {
						break
					}
				}

				go p.SendOneBlock(do.name, do.cli)

				p.mu.Unlock()
			}
			if do.err == TIOUT { //超时，已尝试发送3次
				//认为该消费者掉线
				p.mu.Lock()
				delete(p.clis, do.name) //删除该消费者 考虑是否需要
				//判断是否有消费者存在，若无则关闭协程和文件描述符
				p.mu.Unlock()
			}
		case <-time.After(TOUT * time.Second): //超时
			close <- p
			return
		}
	}
}

func (p *Part) SendOneBlock(name string, cli *client_operations.Client) {
	var in int64
	in = 0
	num := 0
	for {
		p.mu.Lock()
		if in == 0 {
			in = p.start_index
		}

		if _, ok := p.clis[name]; !ok { //不存在，不再负责这个分片
			p.mu.Unlock()
			return
		}

		if int(in) >= len(p.buf_done) {
			in = 0
		}

		if p.buf_done[in] == NOTDO {
			msg, ok1 := p.buffer_msg[in]
			node, ok2 := p.buffer_node[in]

			if !ok1 || !ok2 {
				logger.DEBUG(logger.DError, "get msg and node from buffer the in = %v\n", in)
			}
			p.buf_done[in] = HAVEDO
			p.mu.Unlock()

			data_msg, _ := json.Marshal(msg)
			for {
				err := p.Pub(cli, node, data_msg)

				if err != nil { //超时等原因
					logger.DEBUG(logger.DError, "%v\n", err)
					num++
					if num == AGAIN_NUM { //超时三次，将不再向其发送
						p.part_had <- Done{
							in:   node.Start_index,
							err:  TIOUT,
							name: name,
							cli:  cli,
						}

						p.mu.Lock()
						p.buf_done[in] = NOTDO
						p.mu.Unlock()

						break
					}
				} else {
					p.part_had <- Done{
						in:   node.Start_index,
						err:  OK,
						name: name,
						cli:  cli,
					}
					break
				}
			}
			break
		} else {
			in = p.buffer_node[in].End_index + 1
		}
		p.mu.Unlock()
	}
}

// publish发布
func (p *Part) Pub(cli *client_operations.Client, node Key, data []byte) error {
	resp, err := (*cli).Pub(context.Background(), &api.PubRequest{
		TopicName:  p.topicName,
		PartName:   p.partName,
		StartIndex: node.Start_index,
		EndIndex:   node.End_index,
		Msg:        data,
	})

	if err != nil || !resp.Ret {
		return err
	}

	//修改zookeeper的该PTP的offset

	return nil
}

type MSGS struct {
	start_index int64
	end_index   int64
	size        int8
	array       []byte //由[]Message转byte
}

type Group struct {
	mu        sync.RWMutex
	topicName string
	consumers map[string]bool //map[client name]alive
}

func NewGroup(topicName, cliName string) *Group {
	group := &Group{
		mu:        sync.RWMutex{},
		topicName: topicName,
		consumers: make(map[string]bool),
	}
	group.consumers[cliName] = true
	return group
}

func (g *Group) AddClient(cliName string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.consumers[cliName]
	if ok {
		return errors.New("this client has in this group")
	} else {
		g.consumers[cliName] = true
		return nil
	}
}

func (g *Group) DownClient(cliName string) {
	g.mu.Lock()
	_, ok := g.consumers[cliName]
	if ok {
		g.consumers[cliName] = false
	}
	g.mu.Unlock()
}

func (g *Group) DeleteClient(cliName string) {
	g.mu.Lock()
	_, ok := g.consumers[cliName]
	if ok {
		delete(g.consumers, cliName)
	}
	g.mu.Unlock()
}

type Node struct {
	topicName   string
	partName    string
	option      int8
	file        *File
	fd          os.File
	offset      int64
	start_index int64
}

func NewNode(in info, file *File) *Node {
	node := &Node{
		topicName: in.topicName,
		partName:  in.partName,
		option:    in.option,
		file:      file,
	}

	node.fd = *node.file.OpenFileRead()
	node.offset = -1
	return node
}

func (n *Node) ReadMSGS(in info) (MSGS, error) {
	var err error
	var msgs MSGS
	var out []Message
	if n.offset == -1 || n.start_index != in.offset {
		n.offset, err = n.file.FindOffset(&n.fd, in.offset)
		if err != nil {
			if err == io.EOF {
				n.start_index = in.offset
				return MSGS{}, io.EOF
			}
			logger.DEBUG(logger.DLog2, "%v\n", err.Error())
			return MSGS{}, err
		}
		n.start_index = in.offset
	}
	nums := 0
	finish := func() (MSGS, error) {
		if len(out) == 0 {
			return msgs, io.EOF
		}
		data, err := json.Marshal(out)
		if err != nil {
			return MSGS{}, err
		}
		msgs.array = data
		msgs.size = int8(len(out))
		return msgs, nil
	}
	for nums < int(in.size) {
		node, msg, err := n.file.ReadBytes(&n.fd, n.offset)
		if err != nil {
			if err == io.EOF {
				if len(out) > 0 {
					return finish()
				}
				return msgs, io.EOF
			} else {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return MSGS{}, err
			}
		}
		var blockMsgs []Message
		if err := json.Unmarshal(msg, &blockMsgs); err != nil {
			return MSGS{}, err
		}
		if len(blockMsgs) == 0 {
			return finish()
		}
		if nums == 0 {
			msgs.start_index = blockMsgs[0].Index
			msgs.end_index = blockMsgs[len(blockMsgs)-1].Index
		}
		nums += len(blockMsgs)
		n.offset += int64(NODE_SIZE) + node.Size
		out = append(out, blockMsgs...)
		msgs.end_index = blockMsgs[len(blockMsgs)-1].Index
		n.start_index = msgs.end_index + 1
	}

	return finish()
}
