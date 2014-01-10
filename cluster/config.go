package cluster

type Config struct {
	Address     string   `json:"address"`
	Description string   `json:"description"`
	Nodes       []string `json:"nodes"`
}
