// decl: common data structure declarations

package common

type Node struct {
	Hostname string `json:"hostname"`
	Port     int16  `json:"port"`
	Type     string `json:"type"`
}

func GetMasterNodeData(hostname string, port int) Node {
	return Node{
		Hostname: hostname,
		Port:     int16(port),
		Type:     "master",
	}
}

func GetWorkerNodeData(hostname string, port int) Node {
	return Node{
		Hostname: hostname,
		Port:     int16(port),
		Type:     "worker",
	}
}
