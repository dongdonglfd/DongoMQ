package main

import (
	"DongoMQ/client/clients"
	"fmt"
	"testing"
	"time"
)

// 测试前我们需要对zookeeper和各个集群进行初始化一些数据方便后续的测试
func TestInit1(t *testing.T) {
	fmt.Println("Init: init status for test")

	messages := []string{
		"18211673055", "12345678910",
		"12345678911", "12345678912", "12345678913",
		"12345678914", "12345678915", "12345678916",
		"12345678917", "12345678918", "12345678919",
	}

	zkServer := StartZKServer(t)
	time.Sleep(1 * time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1 * time.Second)

	producer := NewProducerAndStart(t, ":7878", "producer1")
	time.Sleep(1 * time.Second)

	//创建topic
	fmt.Println("Producer Create a Topic")
	err := producer.CreateTopic("phone_number")
	if err != nil {
		fmt.Println(err.Error())
	}

	//每个topic创建partition
	fmt.Println("Producer Create a Topic/Partition")
	err = producer.CreatePart("phone_number", "shanghai")
	if err != nil {
		fmt.Println(err.Error())
	}

	//将partition设置状态
	err = producer.SetPartitionState("phone_number", "shanghai", -1, 3)
	if err != nil {
		fmt.Println(err.Error())
	}

	//等待领导者选出
	time.Sleep(5 * time.Second)

	fmt.Println("Producer push message to partition")
	for _, message := range messages {
		err = producer.Push(clients.Message{
			Topic:     "phone_number",
			Partition: "shanghai",
			Msg:       []byte(message),
		}, -1)
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	time.Sleep(30 * time.Second)
	//向partitions中生产一些信息

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println(" ... Init Successfully")
}
