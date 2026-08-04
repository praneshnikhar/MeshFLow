package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

type Config struct {
	NodeName       string
	BindAddr       string
	BindPort       int
	GossipPort     int
	PubSubPort     int
	APIPort        int
	SeedNodes      []string
	DataDir        string
	GossipInterval time.Duration
}

func Default() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		NodeName:       hostname,
		BindAddr:       "0.0.0.0",
		BindPort:       7946,
		GossipPort:     7947,
		PubSubPort:     4222,
		APIPort:        8080,
		SeedNodes:      []string{},
		DataDir:        "./data",
		GossipInterval: 500 * time.Millisecond,
	}
}

func FromEnv() *Config {
	c := Default()
	if v := os.Getenv("MESHFLOW_NAME"); v != "" {
		c.NodeName = v
	}
	if v := os.Getenv("MESHFLOW_BIND_ADDR"); v != "" {
		c.BindAddr = v
	}
	if v := os.Getenv("MESHFLOW_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.BindPort = p
		}
	}
	if v := os.Getenv("MESHFLOW_GOSSIP_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.GossipPort = p
		}
	}
	if v := os.Getenv("MESHFLOW_PUBSUB_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.PubSubPort = p
		}
	}
	if v := os.Getenv("MESHFLOW_API_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.APIPort = p
		}
	}
	if v := os.Getenv("MESHFLOW_SEEDS"); v != "" {
		c.SeedNodes = splitCSV(v)
	}
	if v := os.Getenv("MESHFLOW_DATA_DIR"); v != "" {
		c.DataDir = v
	}
	return c
}

func (c *Config) AdvertiseAddr() string {
	return fmt.Sprintf("%s:%d", c.BindAddr, c.BindPort)
}

func (c *Config) GossipAddr() string {
	return fmt.Sprintf("%s:%d", c.BindAddr, c.GossipPort)
}

func (c *Config) PubSubAddr() string {
	return fmt.Sprintf("%s:%d", c.BindAddr, c.PubSubPort)
}

func (c *Config) ResolveAdvertiseAddr() string {
	if c.BindAddr != "0.0.0.0" && c.BindAddr != "" {
		return c.AdvertiseAddr()
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return c.AdvertiseAddr()
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			return fmt.Sprintf("%s:%d", ipnet.IP.String(), c.BindPort)
		}
	}
	return c.AdvertiseAddr()
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	current := ""
	for _, ch := range s {
		if ch == ',' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}
