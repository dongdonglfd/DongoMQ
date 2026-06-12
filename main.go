package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	mqserver "DongoMQ/server"
	"DongoMQ/zookeeper"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, err)
		usage()
		os.Exit(2)
	}

	zkinfo := zookeeper.ZkInfo{
		HostPorts: splitCSV(cfg.zkHosts),
		Timeout:   cfg.zkTimeout,
		Root:      cfg.zkRoot,
	}

	if err := checkZooKeeper(zkinfo.HostPorts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	servers := start(cfg, zkinfo)
	waitForShutdown(servers)
}

type config struct {
	mode        string
	zkHosts     string
	zkRoot      string
	zkTimeout   int
	zkServer    string
	brokerName  string
	brokerMe    int
	brokerAddr  string
	raftAddr    string
	brokers     int
	brokerPorts string
	raftPorts   string
}

func defaultConfig() config {
	return config{
		mode:        "all",
		zkHosts:     "127.0.0.1:2181",
		zkRoot:      "/DongoMQ",
		zkTimeout:   20,
		zkServer:    ":7878",
		brokerName:  "Broker0",
		brokerMe:    0,
		brokerAddr:  ":7774",
		raftAddr:    ":7331",
		brokers:     3,
		brokerPorts: ":7774,:7775,:7776",
		raftPorts:   ":7331,:7332,:7333",
	}
}

func parseConfig(args []string) (config, error) {
	cfg := defaultConfig()

	flags := flag.NewFlagSet("DongoMQ", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&cfg.mode, "mode", cfg.mode, "startup mode: all, zkserver, broker")
	flags.StringVar(&cfg.zkHosts, "zk", cfg.zkHosts, "comma-separated ZooKeeper host:port list")
	flags.StringVar(&cfg.zkRoot, "root", cfg.zkRoot, "ZooKeeper root path")
	flags.IntVar(&cfg.zkTimeout, "zk-timeout", cfg.zkTimeout, "ZooKeeper timeout in seconds")
	flags.StringVar(&cfg.zkServer, "zkserver", cfg.zkServer, "zkserver RPC address")
	flags.StringVar(&cfg.brokerName, "broker-name", cfg.brokerName, "broker name for -mode broker")
	flags.IntVar(&cfg.brokerMe, "broker-me", cfg.brokerMe, "broker raft index for -mode broker")
	flags.StringVar(&cfg.brokerAddr, "broker", cfg.brokerAddr, "broker RPC address for -mode broker")
	flags.StringVar(&cfg.raftAddr, "raft", cfg.raftAddr, "raft RPC address for -mode broker")
	flags.IntVar(&cfg.brokers, "brokers", cfg.brokers, "number of brokers for -mode all")
	flags.StringVar(&cfg.brokerPorts, "broker-ports", cfg.brokerPorts, "comma-separated broker RPC addresses for -mode all")
	flags.StringVar(&cfg.raftPorts, "raft-ports", cfg.raftPorts, "comma-separated raft RPC addresses for -mode all")

	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	if flags.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}

	switch cfg.mode {
	case "all", "zkserver", "broker":
	default:
		return cfg, fmt.Errorf("invalid -mode %q", cfg.mode)
	}
	if cfg.brokers <= 0 {
		return cfg, fmt.Errorf("-brokers must be greater than 0")
	}
	return cfg, nil
}

func start(cfg config, zkinfo zookeeper.ZkInfo) []*mqserver.RPCServer {
	switch cfg.mode {
	case "zkserver":
		fmt.Printf("starting zkserver on %s, zookeeper=%s\n", cfg.zkServer, cfg.zkHosts)
		return []*mqserver.RPCServer{startZKServer(zkinfo, cfg)}
	case "broker":
		fmt.Printf("starting broker %s on %s, raft=%s, zkserver=%s\n", cfg.brokerName, cfg.brokerAddr, cfg.raftAddr, cfg.zkServer)
		return []*mqserver.RPCServer{startBroker(zkinfo, cfg, cfg.brokerName, cfg.brokerMe, cfg.brokerAddr, cfg.raftAddr)}
	case "all":
		return startAll(zkinfo, cfg)
	default:
		panic("unreachable")
	}
}

func startAll(zkinfo zookeeper.ZkInfo, cfg config) []*mqserver.RPCServer {
	brokerPorts := splitCSV(cfg.brokerPorts)
	raftPorts := splitCSV(cfg.raftPorts)
	if len(brokerPorts) < cfg.brokers || len(raftPorts) < cfg.brokers {
		fmt.Fprintf(os.Stderr, "-brokers=%d requires at least %d broker and raft ports\n", cfg.brokers, cfg.brokers)
		os.Exit(2)
	}

	fmt.Printf("starting zkserver on %s, zookeeper=%s\n", cfg.zkServer, cfg.zkHosts)
	servers := []*mqserver.RPCServer{startZKServer(zkinfo, cfg)}
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < cfg.brokers; i++ {
		name := "Broker" + strconv.Itoa(i)
		fmt.Printf("starting broker %s on %s, raft=%s\n", name, brokerPorts[i], raftPorts[i])
		servers = append(servers, startBroker(zkinfo, cfg, name, i, brokerPorts[i], raftPorts[i]))
	}
	return servers
}

func startZKServer(zkinfo zookeeper.ZkInfo, cfg config) *mqserver.RPCServer {
	return mqserver.NewZKServerAndStart(zkinfo, mqserver.Options{
		Name:               "ZKServer",
		Tag:                mqserver.ZKBROKER,
		ZKServer_Host_Port: cfg.zkServer,
	})
}

func startBroker(zkinfo zookeeper.ZkInfo, cfg config, name string, me int, brokerAddr, raftAddr string) *mqserver.RPCServer {
	return mqserver.NewBrokerAndStart(zkinfo, mqserver.Options{
		Me:                 me,
		Name:               name,
		Tag:                mqserver.BROKER,
		ZKServer_Host_Port: cfg.zkServer,
		Broker_Host_Port:   brokerAddr,
		Raft_Host_Port:     raftAddr,
	})
}

func waitForShutdown(servers []*mqserver.RPCServer) {
	fmt.Println("DongoMQ is running. Press Ctrl+C to stop.")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	for _, srv := range servers {
		srv.ShutDown_server()
	}
	fmt.Println("DongoMQ stopped.")
}

func checkZooKeeper(hosts []string) error {
	if len(hosts) == 0 {
		return fmt.Errorf("no ZooKeeper hosts configured")
	}

	errs := make([]string, 0, len(hosts))
	for _, host := range hosts {
		conn, err := net.DialTimeout("tcp", host, 2*time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		errs = append(errs, fmt.Sprintf("%s: %v", host, err))
	}
	return fmt.Errorf("cannot connect to ZooKeeper; start ZooKeeper first or pass -zk host:port (%s)", strings.Join(errs, "; "))
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: go run . [options]")
	fmt.Fprintln(os.Stderr, "  -mode all|zkserver|broker")
	fmt.Fprintln(os.Stderr, "  -zk 127.0.0.1:2181[,host:port...]")
	fmt.Fprintln(os.Stderr, "  -zkserver :7878")
	fmt.Fprintln(os.Stderr, "  -broker :7774")
	fmt.Fprintln(os.Stderr, "  -raft :7331")
	fmt.Fprintln(os.Stderr, "  -brokers 3")
}
