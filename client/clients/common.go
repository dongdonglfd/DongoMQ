package clients

import "net"

type PartKey struct {
	Name       string `json:"name"`
	BrokerName string `json:"broker_name"`
	Broker_H_P string `json:"broker_h_p"`
	Offset     int64  `json:"offset"`
	Err        string `json:"err"`
}

type Parts struct {
	PartKeys []PartKey `json:"part_keys"`
}

type BrokerInfo struct {
	Name      string `json:"name"`
	Host_port string `json:"host_port"`
}

func GetIpport() string {
	interfaces, err := net.Interfaces()
	ipport := ""
	if err != nil {
		panic("Poor soul,here is what you got: " + err.Error())
	}
	for _, inter := range interfaces {
		mac := inter.HardwareAddr //获取本机MAC地址
		ipport += mac.String()
	}
	return ipport
}
