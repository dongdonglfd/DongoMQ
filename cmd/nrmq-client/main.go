package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"DongoMQ/client/clients"
	"DongoMQ/server"
)

const (
	modeProduce = "produce"
	modePull    = "pull"
	modeDemo    = "demo"
)

type config struct {
	mode       string
	zkserver   string
	topic      string
	part       string
	producer   string
	consumer   string
	consumerIP string
	messages   string
	ack        int
	replicas   int
	offset     int64
	size       int
	waitLeader time.Duration
	create     bool
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	switch cfg.mode {
	case modeProduce:
		err = produce(cfg)
	case modePull:
		err = pull(cfg)
	case modeDemo:
		if err = produce(cfg); err == nil {
			err = pull(cfg)
		}
	default:
		err = fmt.Errorf("unknown mode %q", cfg.mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		mode:       modeDemo,
		zkserver:   ":7878",
		topic:      "phone_number",
		part:       "xian",
		producer:   "producer-demo",
		consumer:   "consumer-demo",
		consumerIP: ":7881",
		messages:   "hello DongoMQ",
		ack:        1,
		replicas:   3,
		offset:     0,
		size:       10,
		waitLeader: 5 * time.Second,
		create:     true,
	}

	flags := flag.NewFlagSet("DongoMQ-client", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&cfg.mode, "mode", cfg.mode, "client mode: produce, pull, demo")
	flags.StringVar(&cfg.zkserver, "zkserver", cfg.zkserver, "DongoMQ zkserver RPC address")
	flags.StringVar(&cfg.topic, "topic", cfg.topic, "topic name")
	flags.StringVar(&cfg.part, "part", cfg.part, "partition name")
	flags.StringVar(&cfg.producer, "producer", cfg.producer, "producer name")
	flags.StringVar(&cfg.consumer, "consumer", cfg.consumer, "consumer name")
	flags.StringVar(&cfg.consumerIP, "consumer-port", cfg.consumerIP, "consumer callback RPC address")
	flags.StringVar(&cfg.messages, "messages", cfg.messages, "comma-separated messages to produce")
	flags.IntVar(&cfg.ack, "ack", cfg.ack, "producer ack mode: -1 raft, 0 fetch async, 1 fetch leader write")
	flags.IntVar(&cfg.replicas, "replicas", cfg.replicas, "replica count for SetPartitionState")
	flags.Int64Var(&cfg.offset, "offset", cfg.offset, "pull offset")
	flags.IntVar(&cfg.size, "size", cfg.size, "max messages to pull")
	flags.DurationVar(&cfg.waitLeader, "wait-leader", cfg.waitLeader, "time to wait after setting partition state")
	flags.BoolVar(&cfg.create, "create", cfg.create, "create topic/partition and set partition state before producing")

	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	if flags.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if cfg.size <= 0 || cfg.size > 127 {
		return cfg, fmt.Errorf("-size must be between 1 and 127")
	}
	if cfg.mode != modeProduce && cfg.mode != modePull && cfg.mode != modeDemo {
		return cfg, fmt.Errorf("-mode must be one of produce, pull, demo")
	}
	return cfg, nil
}

func produce(cfg config) error {
	producer, err := clients.NewProducer(cfg.zkserver, cfg.producer)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}

	if cfg.create {
		if err := producer.CreateTopic(cfg.topic); err != nil {
			return fmt.Errorf("create topic %q: %w", cfg.topic, err)
		}
		if err := producer.CreatePart(cfg.topic, cfg.part); err != nil {
			return fmt.Errorf("create partition %q/%q: %w", cfg.topic, cfg.part, err)
		}
		if err := producer.SetPartitionState(cfg.topic, cfg.part, int8(cfg.ack), int8(cfg.replicas)); err != nil {
			return fmt.Errorf("set partition state: %w", err)
		}
		if cfg.waitLeader > 0 {
			time.Sleep(cfg.waitLeader)
		}
	}

	for _, msg := range splitMessages(cfg.messages) {
		err := producer.Push(clients.Message{
			Topic:     cfg.topic,
			Partition: cfg.part,
			Msg:       []byte(msg),
		}, cfg.ack)
		if err != nil {
			return fmt.Errorf("push message %q: %w", msg, err)
		}
		fmt.Printf("produced %s/%s: %s\n", cfg.topic, cfg.part, msg)
	}
	return nil
}

func pull(cfg config) error {
	consumer, err := clients.NewConsumer(cfg.zkserver, cfg.consumer, cfg.consumerIP)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	if err := consumer.Sub(cfg.topic, cfg.part, server.TOPIC_KEY_PSB_PUSH); err != nil {
		return fmt.Errorf("subscribe %s/%s: %w", cfg.topic, cfg.part, err)
	}

	parts, ret, err := consumer.StartGet(clients.Info{
		Offset: cfg.offset,
		Topic:  cfg.topic,
		Part:   cfg.part,
		Option: server.TOPIC_KEY_PSB_PULL,
		Size:   int8(cfg.size),
	})
	if err != nil {
		return fmt.Errorf("start get: %s %w", ret, err)
	}
	if len(parts) == 0 {
		return fmt.Errorf("no broker returned for %s/%s", cfg.topic, cfg.part)
	}

	for _, part := range parts {
		if part.Err != "ok" {
			fmt.Printf("skip broker result %s: %s\n", part.BrokerName, part.Err)
			continue
		}
		cli, err := consumer.GetCli(part)
		if err != nil {
			return fmt.Errorf("connect broker %s: %w", part.BrokerName, err)
		}
		info := clients.NewInfo(cfg.offset, cfg.topic, cfg.part)
		info.Cli = cli
		info.Option = server.TOPIC_KEY_PSB_PULL
		info.Size = int8(cfg.size)

		start, end, msgs, err := consumer.Pull(info)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("no messages available from %s at offset %d\n", part.BrokerName, cfg.offset)
				continue
			}
			return fmt.Errorf("pull from broker %s: %w", part.BrokerName, err)
		}
		fmt.Printf("pulled range [%d, %d) from %s\n", start, end, part.BrokerName)
		for _, msg := range msgs {
			fmt.Printf("%d %s/%s: %s\n", msg.Index, msg.Topic_name, msg.Part_name, string(msg.Msg))
		}
	}
	return nil
}

func splitMessages(value string) []string {
	raw := strings.Split(value, ",")
	out := make([]string, 0, len(raw))
	for _, msg := range raw {
		msg = strings.TrimSpace(msg)
		if msg != "" {
			out = append(out, msg)
		}
	}
	return out
}
