package main

import (
	"DongoMQ/client/clients"
	"DongoMQ/logger"
	"DongoMQ/server"
	"fmt"
	"testing"
	"time"
)

const (
	PTP = 1
	PSB = 3
)

func TestConsumer1(t *testing.T) {
	fmt.Println("Test:consumer Subscription")

	zkServer := StartZKServer(t)
	time.Sleep(1 * time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1 * time.Second)

	consumer := NewConsumerAndStart(t, ":7881", ":7878", "consumer1")
	time.Sleep(1 * time.Second)

	fmt.Println("Consumer Sub a Topic")
	err := consumer.Sub("phone_number", "北京", PTP)
	if err != nil {
		t.Fatal(err.Error())
	}

	consumer.Shutdown_server()
	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("... Passed")
}

func TestPullPTP(t *testing.T) {
	fmt.Println("Test:Consumer pull message used ptp_pull")

	zkServer := StartZKServer(t)
	time.Sleep(1 * time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1 * time.Second)

	consumer := NewConsumerAndStart(t, ":7881", ":7878", "consumer1")
	time.Sleep(1 * time.Second)

	fmt.Println("Consumer Sub a Topic")
	err := consumer.Sub("phone_number", "xian", PTP)
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("Consumer startGet")
	parts, ret, err := consumer.StartGet(clients.Info{
		Topic:  "phone_number",
		Part:   "xian",
		Option: server.TOPIC_NIL_PTP_PULL,
	})
	if err != nil {
		logger.DEBUG(logger.DError, ret+" "+err.Error())
	}

	fmt.Println("Consumer pull msg")
	for _, part := range parts {
		cli, err := consumer.GetCli(part)
		if err != nil {
			t.Fatal(err.Error())
		}
		info := clients.NewInfo(0, "phone_number", "xian")
		info.Cli = cli
		info.Option = server.TOPIC_NIL_PTP_PULL
		info.Size = 10
		start, end, msgs, err := consumer.Pull(info)
		if err != nil {
			t.Fatal(err.Error())
		}

		fmt.Println("start ", start, "end ", end)
		for _, msg := range msgs {
			fmt.Println(msg.Topic_name, msg.Part_name, msg.Index, string(msg.Msg))
		}
	}

	time.Sleep(time.Second * 30)

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("... Passed")
}

func TestPullPSB(t *testing.T) {
	fmt.Println("Test:consumer pull message used psb_pull")

	zkServer := StartZKServer(t)
	time.Sleep(1 * time.Second)

	brokers := StartBrokers(t, 3)
	time.Sleep(1 * time.Second)

	consumer := NewConsumerAndStart(t, ":7881", ":7878", "consumer1")
	time.Sleep(1 * time.Second)

	parts, ret, err := consumer.StartGet(clients.Info{
		Topic:  "phone_number",
		Part:   "xian",
		Option: server.TOPIC_KEY_PSB_PULL,
	})
	if err != nil {
		t.Fatal(ret, err.Error())
	}

	for _, part := range parts {
		cli, err := consumer.GetCli(part)
		if err != nil {
			t.Fatal(err.Error())
		}
		info := clients.NewInfo(0, "phone_number", "xian")
		info.Cli = cli
		info.Option = server.TOPIC_KEY_PSB_PULL
		info.Size = 10
		start, end, msgs, err := consumer.Pull(info)
		if err != nil {
			t.Fatal(err.Error())
		}

		fmt.Println("start ", start, "end ", end)
		for _, msg := range msgs {
			fmt.Println(msg.Topic_name, msg.Part_name, msg.Index, string(msg.Msg))
		}
	}

	time.Sleep(time.Second * 30)

	ShutDownBrokers(brokers)
	ShutDownZKServer(zkServer)
	fmt.Println("... Passed")
}
