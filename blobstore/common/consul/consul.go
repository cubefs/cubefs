// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package consul

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/hashicorp/consul/api"
)

type Config struct {
	// service
	ConsulAddr  string   `json:"consul_addr"`
	ServiceName string   `json:"service_name"`
	Node        string   `json:"node"`
	Tags        []string `json:"tags"`

	// health
	Interval    string `json:"interval"`
	Timeout     string `json:"timeout"`
	ServiceIP   string `json:"service_ip"`
	ServicePort int    `json:"service_port"`
	// health check port range in [ min(HealthPort), max(HealthPort) ]
	HealthPort [2]int `json:"health_port"`
}

const healthRoutine = "/healthCheck/"

type Client struct {
	healthServer *http.Server
	*api.Client
	*Config
}

func ServiceRegister(bindAddr string, cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = defaultConfig()
	} else {
		initConfig(cfg)
	}
	if bindAddr == "" {
		err := errors.New("bindAddr should not be empty")
		return nil, err
	}
	_, port, err := net.SplitHostPort(bindAddr)
	if err != nil {
		log.Error("invalid port error: ", err)
		return nil, err
	}
	cfg.ServicePort, err = strconv.Atoi(port)

	if err != nil {
		return nil, err
	}
	defConfig := api.DefaultConfig()

	defConfig.Address = cfg.ConsulAddr

	client, err := api.NewClient(defConfig)
	if err != nil {
		return nil, err
	}
	c := &Client{Config: cfg, Client: client}

	registration, err := c.initConsulService()
	if err != nil {
		return nil, err
	}
	err = c.Agent().ServiceRegister(registration)
	if err != nil {
		log.Errorf("failed in registering server err:%v", err)
	}
	return c, err
}

func (c *Client) initConsulService() (registration *api.AgentServiceRegistration, err error) {
	registration = new(api.AgentServiceRegistration)
	registration.ID = c.getId()
	registration.Name = c.ServiceName
	registration.Port = c.ServicePort
	registration.Tags = c.Tags
	check := new(api.AgentServiceCheck)

	if c.ServiceIP != "" {
		registration.Address = c.ServiceIP
	}

	patten := healthRoutine + strconv.Itoa(c.ServicePort)

	serv, port := startHttpServerForHealthyCheck(registration.Address, patten, c.Config.HealthPort)

	c.healthServer = serv

	healthCheckAddr := "0.0.0.0"
	if registration.Address != "" {
		healthCheckAddr = registration.Address
	}

	healthCheckUrl := fmt.Sprintf("http://%v:%v%v", healthCheckAddr, port, patten)

	log.Infof("health server: %v", healthCheckUrl)

	check.HTTP = healthCheckUrl
	check.Interval = c.Interval
	check.Timeout = c.Timeout

	registration.Check = check

	return registration, nil
}

func (c *Client) Close() {
	if c.healthServer != nil {
		c.Agent().ServiceDeregister(c.getId())
		c.healthServer.Close()
		log.Info("registerer service closed")
	}
}

func defaultConfig() (cfg *Config) {
	cfg = &Config{}
	initConfig(cfg)
	return
}

func initConfig(cfg *Config) {
	if cfg.ServiceName == "" {
		path := os.Args[0]
		program := filepath.Base(path)
		cfg.ServiceName = program
	}
	if cfg.ServiceIP == "" {
		cfg.ServiceIP = "127.0.0.1"
		log.Errorf("service IP not config, serviceIP init as: %v", cfg.ServiceIP)
	}

	cfg.ServiceIP = parseIP(cfg.ServiceIP)

	if cfg.ConsulAddr == "" {
		cfg.ConsulAddr = "127.0.0.1:8500"
	}
	if cfg.Timeout == "" {
		cfg.Timeout = "3s"
	}
	if cfg.Interval == "" {
		cfg.Interval = "30s"
	}
	if cfg.HealthPort[0] > cfg.HealthPort[1] {
		cfg.HealthPort[0], cfg.HealthPort[1] = cfg.HealthPort[1], cfg.HealthPort[0]
	}
}

func parseIP(ip string) (host string) {
	if !strings.HasPrefix(ip, "http") {
		return ip
	}
	u, err := url.Parse(host)
	if err != nil {
		return ip
	}
	return u.Host
}

func (c *Client) getId() string {
	if c.Node == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Error("host name not exist error:", err)
			hostname = ""
		}
		c.Node = hostname
	}
	return c.Node + "-" + c.ServiceName + "-" + strconv.Itoa(c.ServicePort)
}

func startHttpServerForHealthyCheck(ip, patten string, checkPorts [2]int) (srv *http.Server, port int) {
	if ip == "" {
		ip = "127.0.0.1"
	}
	var ln net.Listener
	var err error

	for checkPort := checkPorts[0]; checkPort <= checkPorts[1]; checkPort++ {
		addr := ip + ":" + strconv.Itoa(checkPort)
		ln, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}
		log.Errorf("server listen error: %v", err)
	}
	if err != nil {
		log.Fatalf("server listen error: %v", err)
	}

	srv = &http.Server{}
	srv.Addr = ln.Addr().String()
	port = ln.Addr().(*net.TCPAddr).Port
	log.Info("start health check server on: ", srv.Addr)
	http.HandleFunc(patten, healthCheck)
	go func() {
		httpError := srv.Serve(ln.(*net.TCPListener))
		if httpError != nil && httpError != http.ErrServerClosed {
			log.Fatalf("health server HTTP error: ", httpError)
		}
		log.Info("health check server exit")
	}()
	return
}

func checkConsulAgentRunning(client *api.Client) error {
	if _, err := client.Catalog().Datacenters(); err != nil {
		log.Errorf("consul not running error: %v", err)
		return err
	}
	return nil
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
