package zookeeper

import (
	"DongoMQ/logger"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/go-zookeeper/zk"
)

var (
	BNodePath  = "%v/%v"                     // BrokerRoot/BrokerName
	TNodePath  = "%v/%v"                     // TopicRoot/TopicName
	PNodePath  = "%v/%v/Partitions/%v"       // TNodePath/Partitions/PartName
	SNodePath  = "%v/%v/Subscriptions/%v"    // TNodePath/Subscriptions/SubscriptionName
	BlNodePath = "%v/%v/Partitions/%v/%v"    // PNodePath/BlockName
	RNodePath  = "%v/%v/Partitions/%v/%v/%v" // BlNodePath/ReplicaName
)

type ZK struct {
	Conn *zk.Conn

	Root       string
	BrokerRoot string
	TopicRoot  string
}

type ZkInfo struct {
	HostPorts []string //
	Timeout   int      //zookeeper连接的超时时间
	Root      string
}

func NewZK(info ZkInfo) (*ZK, error) {
	conn, _, err := zk.Connect(info.HostPorts, time.Duration(info.Timeout)*time.Second)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	z := &ZK{
		Conn:       conn,
		Root:       info.Root,
		BrokerRoot: info.Root + "/Brokers",
		TopicRoot:  info.Root + "/Topics",
	}

	ok, _, err := z.Conn.Exists(z.Root)
	if ok {
		logger.DEBUG(logger.DLog, "the %v already exists.\n", z.Root)
	} else {
		_, err = z.Conn.Create(z.Root, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the zkRoot %v create failed %v\n", z.Root, err.Error())
			return nil, err
		}
	}
	ok, _, err = z.Conn.Exists(z.BrokerRoot)
	if ok {
		logger.DEBUG(logger.DLog, "the %v already exists.\n", z.BrokerRoot)
	} else {
		_, err = z.Conn.Create(z.BrokerRoot, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the zkBroker %v create failed %v\n", z.BrokerRoot, err.Error())
			return nil, err
		}
	}
	ok, _, err = z.Conn.Exists(z.TopicRoot)
	if ok {
		logger.DEBUG(logger.DLog, "the %v already exists.\n", z.TopicRoot)
	} else {
		_, err = z.Conn.Create(z.TopicRoot, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the zkTopicRoot %v create failed %v\n", z.TopicRoot, err.Error())
			return nil, err
		}
	}

	return z, nil
}

type BrokerNode struct {
	Name         string `json:"name"`
	BrokHostPort string `json:"brokhostport"`
	RaftHostPort string `json:"rafthostport"`
	Me           int    `json:"me"`
	Pnum         int    `json:"pnum"` //partition数量
}

type TopicNode struct {
	Name    string `json:"name"`
	PartNum int    `json:"part_num"` //partition数量
}

type PartitionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topicName"`
	Index     int64  `json:"index"`
	Option    int8   `json:"option"` //partition的状态
	RepNum    int8   `json:"repNum"` //副本数量
	PTPoffset int64  `json:"ptpoffset"`
}

type SubscriptionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topicName"`
	PartName  string `json:"partName"`
	Subtype   int8   `json:"subtype"`
	Groups    []byte `json:"groups"`
}

type BlockNode struct {
	Name        string `json:"name"`
	FileName    string `json:"fileName"`
	TopicName   string `json:"topicName"`
	PartName    string `json:"partName"`
	StartOffset int64  `json:"startOffset"`
	EndOffset   int64  `json:"endOffset"`

	LeaderBroker string `json:"leaderBroker"`
}

type ReplicaNode struct {
	Name        string `json:"name"`
	TopicName   string `json:"topicName"`
	PartName    string `json:"partName"`
	BlockName   string `json:"blockName"`
	StartOffset int64  `json:"startOffset"`
	EndOffset   int64  `json:"endOffset"`
	BrokerName  string `json:"brokerName"`
}

type SuberNode struct {
	name      string `json:"name"`
	TopicName string `json:"topicName"`
	PartName  string `json:"partName"`
}

type Map struct {
	Consumers map[string]bool `json:"consumers"`
}

func (z *ZK) RegisterNode(znode interface{}) (err error) {
	path := ""
	var data []byte
	var bnode BrokerNode
	var tnode TopicNode
	var pnode PartitionNode
	var snode SubscriptionNode
	var blnode BlockNode
	var rnode ReplicaNode

	i := reflect.TypeOf(znode)
	switch i.Name() {
	case "BrokerNode":
		bnode = znode.(BrokerNode)
		path = fmt.Sprintf(BNodePath, z.BrokerRoot, bnode.Name)
		data, err = json.Marshal(bnode)
	case "TopicNode":
		tnode = znode.(TopicNode)
		path = fmt.Sprintf(TNodePath, z.TopicRoot, tnode.Name)
		data, err = json.Marshal(tnode)
	case "PartitionNode":
		pnode = znode.(PartitionNode)
		path = fmt.Sprintf(PNodePath, z.TopicRoot, pnode.TopicName, pnode.Name)
		data, err = json.Marshal(pnode)
	case "SubscriptionNode":
		snode = znode.(SubscriptionNode)
		path = fmt.Sprintf(SNodePath, z.TopicRoot, snode.TopicName, snode.Name)
		data, err = json.Marshal(snode)
	case "BlockNode":
		blnode = znode.(BlockNode)
		path = fmt.Sprintf(BlNodePath, z.TopicRoot, blnode.TopicName, blnode.PartName, blnode.Name)
		data, err = json.Marshal(blnode)
	case "ReplicaNode":
		rnode = znode.(ReplicaNode)
		path = fmt.Sprintf(RNodePath, z.TopicRoot, rnode.TopicName, rnode.PartName, rnode.BlockName, rnode.Name)
		data, err = json.Marshal(rnode)
	}

	if err != nil {
		logger.DEBUG(logger.DError, "the node %v turn json failed %v\n", path, err.Error())
		return err
	}
	ok, _, _ := z.Conn.Exists(path)
	if ok {
		logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", path)
		_, sata, _ := z.Conn.Get(path)
		z.Conn.Set(path, data, sata.Version)
	} else {
		_, err = z.Conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v create failed %v\n", path, err.Error())
			return err
		}
	}

	if i.Name() == "TopicNode" {
		partitionPath := path + "/" + "Partitions"
		ok, _, err := z.Conn.Exists(partitionPath)
		if ok {
			logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", partitionPath)
			return err
		}
		_, err = z.Conn.Create(partitionPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v create failed %v\n", partitionPath, err.Error())
			return err
		}

		subscriptionPath := path + "/" + "Subscriptions"
		ok, _, err = z.Conn.Exists(subscriptionPath)
		if ok {
			logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", subscriptionPath)
			return err
		}
		_, err = z.Conn.Create(subscriptionPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v create failed %v\n", subscriptionPath, err.Error())
			return err
		}
	}

	return nil
}

func (z *ZK) UpdatePartitionNode(pnode PartitionNode) error {
	path := fmt.Sprintf(PNodePath, z.TopicRoot, pnode.TopicName, pnode.Name)
	ok, _, err := z.Conn.Exists(path)
	if !ok {
		return err
	}
	data, err := json.Marshal(pnode)
	if err != nil {
		return err
	}
	_, sate, _ := z.Conn.Get(path)
	_, err = z.Conn.Set(path, data, sate.Version)
	if err != nil {
		return err
	}

	return nil
}

func (z *ZK) UpdateBlockNode(bnode BlockNode) error {
	path := fmt.Sprintf(BlNodePath, z.TopicRoot, bnode.TopicName, bnode.PartName, bnode.Name)
	ok, _, err := z.Conn.Exists(path)
	if !ok {
		return err
	}
	data, err := json.Marshal(bnode)
	if err != nil {
		return err
	}
	_, sate, _ := z.Conn.Get(path)
	_, err = z.Conn.Set(path, data, sate.Version)
	if err != nil {
		return err
	}
	return nil
}

func (z *ZK) GetPartState(topic_name, part_name string) (PartitionNode, error) {
	var node PartitionNode
	path := fmt.Sprintf(PNodePath, z.TopicRoot, topic_name, part_name)
	ok, _, err := z.Conn.Exists(path)
	if !ok {
		return node, err
	}
	data, _, _ := z.Conn.Get(path)

	err = json.Unmarshal(data, &node)

	return node, nil
}

func (z *ZK) CreateState(name string) error {
	path := z.BrokerRoot + "/" + name + "/state"
	ok, stat, err := z.Conn.Exists(path)
	if err != nil {
		return err
	}
	if ok {
		logger.DEBUG(logger.DLog, "the %v already exists, recreate it.\n", path)
		err = z.Conn.Delete(path, stat.Version)
		if err != nil {
			logger.DEBUG(logger.DLog, "delete broker state %v failed %v\n", path, err.Error())
			return err
		}
	}

	_, err = z.Conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		logger.DEBUG(logger.DLog, "create broker state %v failed %v\n", path, err.Error())
		return err
	}
	return nil
}

type Part struct {
	TopicName     string
	PartName      string
	BrokerName    string
	BrokHost_Port string
	RaftHost_Port string
	PTP_index     int64
	Filename      string
	Err           string
}

// 检查broker是否在线
func (z *ZK) CheckBroker(BrokerName string) bool {
	path := z.BrokerRoot + "/" + BrokerName + "/state"
	ok, _, _ := z.Conn.Exists(path)
	logger.DEBUG(logger.DLog, "state(%v) path is %v\n", ok, path)
	return ok
}

// consumer获取PTP的Brokers //(和PTP的offset)
func (z *ZK) GetBrokers(topic string) ([]Part, error) {
	path := fmt.Sprintf(TNodePath, z.TopicRoot, topic) + "/" + "Partitions"
	ok, _, err := z.Conn.Exists(path)
	if !ok || err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil, err
	}

	var parts []Part
	partitions, _, _ := z.Conn.Children(path)
	for _, part := range partitions {
		PNode, err := z.GetPartitionNode(path + "/" + part)
		if err != nil {
			logger.DEBUG(logger.DError, "get partitionNode fail %v/%v\n", path, part)
			return nil, err
		}
		PTP_index := PNode.PTPoffset

		var max_replica ReplicaNode
		max_replica.EndOffset = 0
		blocks, _, _ := z.Conn.Children(path + "/" + part)
		for _, block := range blocks {
			info, err := z.GetBlockNode(path + "/" + part + "/" + block)
			if err != nil {
				logger.DEBUG(logger.DError, "get block node %v/%v/%v fail,err is %v\n", path, part, block, err.Error())
				continue
			}
			logger.DEBUG(logger.DLog, "the block is %v\n", info)
			if info.StartOffset <= PTP_index && info.EndOffset >= PTP_index {
				Replicas, _, _ := z.Conn.Children(path + "/" + part + "/" + info.Name)
				for _, replica := range Replicas {
					replicaNode, err := z.GetReplicaNode(path + "/" + part + "/" + info.Name + "/" + replica)
					if err != nil {
						logger.DEBUG(logger.DError, "get replica node %v/%v/%v/%v fail,err is %v\n", path, part, info.Name, replica, err.Error())
						continue
					}
					logger.DEBUG(logger.DLog, "path %v replica node %v\n", path+"/"+part+"/"+info.Name+"/"+replica, replicaNode)
					if max_replica.EndOffset == 0 || max_replica.EndOffset <= replicaNode.EndOffset {
						//保证broker在线
						if z.CheckBroker(replicaNode.BrokerName) {
							max_replica = replicaNode
						} else {
							logger.DEBUG(logger.DLog, "the broker %v is not online\n", replicaNode.BrokerName)
						}
					}
				}
				logger.DEBUG(logger.DLog, "the max_replica is %v\n", max_replica)
				var ret string
				if max_replica.Name != "" {
					ret = "ok"
				} else {
					ret = "the brokers are not online"
				}

				//一个partition只取endoffset最大的broker，其他小的broker副本不全面
				broker, err := z.GetBrokerNode(max_replica.BrokerName)
				if err != nil {
					logger.DEBUG(logger.DError, "get broker node %v fail\n", max_replica.BrokerName)
					continue
				}
				parts = append(parts, Part{
					TopicName:     topic,
					PartName:      part,
					BrokerName:    broker.Name,
					BrokHost_Port: broker.BrokHostPort,
					RaftHost_Port: broker.RaftHostPort,
					PTP_index:     PTP_index,
					Filename:      info.FileName,
					Err:           ret,
				})
				break
			}
		}
	}
	return parts, nil
}

func (z *ZK) GetBroker(topic, part string, offset int64) (parts []Part, err error) {
	part_path := fmt.Sprintf(PNodePath, z.TopicRoot, topic, part)
	ok, _, err := z.Conn.Exists(part_path)
	if !ok || err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil, err
	}

	var max_replica ReplicaNode
	max_replica.EndOffset = 0
	blocks, _, _ := z.Conn.Children(part_path)
	for _, block := range blocks {
		info, err := z.GetBlockNode(part_path + "/" + block)
		if err != nil {
			logger.DEBUG(logger.DError, "get block node %v/%v fail\n", part_path, block)
			continue
		}

		if info.StartOffset <= offset && info.EndOffset >= offset {
			filename := info.FileName
			if block == "NowBlock" {
				filename = "NowBlock.txt"
				if info.LeaderBroker != "" && z.CheckBroker(info.LeaderBroker) {
					broker, err := z.GetBrokerNode(info.LeaderBroker)
					if err == nil {
						parts = append(parts, Part{
							TopicName:     topic,
							PartName:      part,
							BrokerName:    broker.Name,
							BrokHost_Port: broker.BrokHostPort,
							RaftHost_Port: broker.RaftHostPort,
							Filename:      filename,
							Err:           "ok",
						})
						break
					}
					logger.DEBUG(logger.DError, "get leader broker node %v fail\n", info.LeaderBroker)
				}
			}
			Replicas, _, _ := z.Conn.Children(part_path + "/" + block)
			for _, replica := range Replicas {
				replicaNode, err := z.GetReplicaNode(part_path + "/" + block + "/" + replica)
				if err != nil {
					logger.DEBUG(logger.DError, "get replica node %v/%v/%v fail\n", part_path, block, replica)
					continue
				}
				if max_replica.EndOffset == 0 || max_replica.EndOffset <= replicaNode.EndOffset {
					//保证broker在线
					if z.CheckBroker(replicaNode.BrokerName) {
						max_replica = replicaNode
					}
				}
			}
			var ret string
			if max_replica.Name != "" {
				ret = "ok"
			} else {
				ret = "the brokers are not online"
			}
			//一个partition只取endoffset最大的broker，其他小的broker副本不全面
			broker, err := z.GetBrokerNode(max_replica.BrokerName)
			if err != nil {
				logger.DEBUG(logger.DError, "get broker node %v fail\n", max_replica.BrokerName)
				continue
			}
			parts = append(parts, Part{
				TopicName:     topic,
				PartName:      part,
				BrokerName:    broker.Name,
				BrokHost_Port: broker.BrokHostPort,
				RaftHost_Port: broker.RaftHostPort,
				Filename:      filename,
				Err:           ret,
			})
			break
		}
	}
	return parts, nil
}

type StartGetInfo struct {
	CliName       string
	TopicName     string
	PartitionName string
	Option        int8
}

func (z *ZK) CheckSub(info StartGetInfo) bool {

	//检查该consumer是否订阅了该topic或partition

	return true
}

func (z *ZK) GetBlockSize(topicName, partName string) (int, error) {
	path := fmt.Sprintf(PNodePath, z.TopicRoot, topicName, partName)
	ok, _, err := z.Conn.Exists(path)
	if !ok {
		return 0, err
	}

	parts, _, err := z.Conn.Children(path)
	if err != nil {
		return 0, err
	}
	return len(parts), nil
}

func (z *ZK) GetBrokerNode(name string) (BrokerNode, error) {
	path := z.BrokerRoot + "/" + name
	var bronode BrokerNode
	ok, _, err := z.Conn.Exists(path)
	if !ok {
		return bronode, err
	}
	data, _, _ := z.Conn.Get(path)
	err = json.Unmarshal(data, &bronode)
	return bronode, nil
}

func (z *ZK) GetPartitionNode(path string) (PartitionNode, error) {
	var pnode PartitionNode
	ok, _, err := z.Conn.Exists(path)
	if !ok {
		return pnode, err
	}
	data, _, _ := z.Conn.Get(path)
	err = json.Unmarshal(data, &pnode)

	return pnode, nil
}

func (z *ZK) GetBlockNode(path string) (BlockNode, error) {
	var blocknode BlockNode
	data, _, err := z.Conn.Get(path)
	if err != nil {
		logger.DEBUG(logger.DError, "the block path is %v err is %v\n", path, err.Error())
		return blocknode, err
	}
	json.Unmarshal(data, &blocknode)

	return blocknode, nil
}

func (z *ZK) GetReplicaNode(path string) (ReplicaNode, error) {
	var pnode ReplicaNode
	ok, _, err := z.Conn.Exists(path)
	if !ok {
		if err != nil {
			return pnode, err
		}
		return pnode, fmt.Errorf("replica node %v does not exist", path)
	}
	data, _, _ := z.Conn.Get(path)
	err = json.Unmarshal(data, &pnode)
	return pnode, nil
}

func (z *ZK) GetReplicaNodes(topicName, partName, blockName string) (nodes []ReplicaNode) {
	path := fmt.Sprintf(BlNodePath, z.TopicRoot, topicName, partName, blockName)
	reps, _, _ := z.Conn.Children(path)
	for _, repName := range reps {
		RepNode, err := z.GetReplicaNode(path + "/" + repName)
		if err != nil {
			logger.DEBUG(logger.DError, "the replica node %v doesn't exist", path+"/"+repName)
		} else {
			nodes = append(nodes, RepNode)
		}
	}
	return nodes
}

func (z *ZK) GetPartNowBrokerNode(topicName, partName string) (BrokerNode, BlockNode, error) {
	now_block_path := fmt.Sprintf(BlNodePath, z.TopicRoot, topicName, partName, "NowBlock")
	for {
		NowBlock, err := z.GetBlockNode(now_block_path)
		if err != nil {
			logger.DEBUG(logger.DError, "get blockNode %v fail,err is %v\n", now_block_path, err.Error())
			return BrokerNode{}, BlockNode{}, err
		}

		if NowBlock.LeaderBroker == "" {
			replicas := z.GetReplicaNodes(topicName, partName, NowBlock.Name)
			if len(replicas) == 0 {
				return BrokerNode{}, BlockNode{}, fmt.Errorf("no replica found for %v/%v/%v", topicName, partName, NowBlock.Name)
			}
			NowBlock.LeaderBroker = replicas[0].BrokerName
			if err := z.UpdateBlockNode(NowBlock); err != nil {
				return BrokerNode{}, BlockNode{}, err
			}
		}

		Broker, err := z.GetBrokerNode(NowBlock.LeaderBroker)
		if err != nil {
			logger.DEBUG(logger.DError, "get brokerNode %v fail,err is %v\n", NowBlock.LeaderBroker, err.Error())
			return BrokerNode{}, BlockNode{}, err
		}
		logger.DEBUG(logger.DLog, "the leader broker is %v\n", NowBlock.LeaderBroker)

		ret := z.CheckBroker(Broker.Name)
		if ret {
			return Broker, NowBlock, nil
		} else {
			logger.DEBUG(logger.DLog, "the broker %v is not online", Broker.Name)
			time.Sleep(time.Second * 1)
		}
	}
}

func (z *ZK) GetPartBlockIndex(topicName, partName string) (int64, error) {
	str := z.TopicRoot + "/" + topicName + "/" + "Partitions" + "/" + partName
	node, err := z.GetPartitionNode(str)
	if err != nil {
		logger.DEBUG(logger.DError, "get partition node fail path is %v err is %v\n", str, err.Error())
		return 0, err
	}

	return node.Index, nil
}
