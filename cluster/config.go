package cluster

type Config struct {
	Address string   `json:"address"`
	Nodes   []string `json:"nodes"`
}
