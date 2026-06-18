package main

import (
	"DongoMQ/client/clients"
	"DongoMQ/server"
	"DongoMQ/zookeeper"
	"fmt"
	"strconv"
	"testing"
)

func NewConsumerAndStart(t *testing.T, consumer_port, zkbroker, name string) *clients.Consumer {
	fmt.Println("Start Consumer")

	consumer, err := clients.NewConsumer(zkbroker, name, consumer_port)
	if err != nil {
		t.Fatal(err.Error())
	}
	go consumer.Start_server()

	return consumer
}

func NewProducerAndStart(t *testing.T, zkbroker, name string) *clients.Producer {
	fmt.Println("Start Producer")

	Producer, _ := clients.NewProducer(zkbroker, name)

	return Producer
}

func StartBrokers(t *testing.T, numbers int) (brokers []*server.RPCServer) {
	fmt.Println("Start Brokers")

	zookeeper_port := []string{"127.0.0.1:2181"}
	server_ports := []string{":7774", ":7775", ":7776"}
	raft_ports := []string{":7331", ":7332", ":7333"}

	index := 0
	for index < numbers {
		broker := server.NewBrokerAndStart(zookeeper.ZkInfo{
			HostPorts: zookeeper_port,
			Timeout:   20,
			Root:      "/DongoMQ",
		}, server.Options{
			Me:                 index,
			Name:               "Broker" + strconv.Itoa(index),
			Tag:                server.BROKER,
			Broker_Host_Port:   server_ports[index],
			Raft_Host_Port:     raft_ports[index],
			ZKServer_Host_Port: ":7878",
		})
		index++
		brokers = append(brokers, broker)
	}
	return brokers
}

func StartZKServer(t *testing.T) *server.RPCServer {
	fmt.Println("Start ZKServer")

	zookeeper_port := []string{"127.0.0.1:2181"}
	zkserver := server.NewZKServerAndStart(zookeeper.ZkInfo{
		HostPorts: zookeeper_port,
		Timeout:   20,
		Root:      "/DongoMQ",
	}, server.Options{
		Name:               "ZKServer",
		Tag:                server.ZKBROKER,
		ZKServer_Host_Port: ":7878",
	})

	return zkserver
}

func ShutDownBrokers(brokers []*server.RPCServer) {
	for _, broker := range brokers {
		broker.ShutDown_server()
	}
}

func ShutDownZKServer(zkserver *server.RPCServer) {
	zkserver.ShutDown_server()
}
