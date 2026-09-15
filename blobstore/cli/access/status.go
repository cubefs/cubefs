// Copyright 2026 The CubeFS Authors.
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

package access

import (
	"context"
	"strings"
	"time"

	"github.com/desertbit/grumble"
	"github.com/hashicorp/consul/api"

	"github.com/cubefs/cubefs/blobstore/access/stream"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/flags"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

// accessStatus mirrors the response of GET /access/status on the access
// service, see blobstore/access/server.go. Limit/Pool/Config are local to the
// access process; Clusters/Services are cluster-level snapshots aggregated
// from clustermgr. Only the first target address is queried: any access node
// shares the same cluster view.
type accessStatus struct {
	Limit    stream.Status                           `json:"limit"`
	Pool     resourcepool.Status                     `json:"pool"`
	Config   stream.StreamConfig                     `json:"config"`
	Clusters []*cmapi.ClusterInfo                    `json:"clusters"`
	Services map[proto.ClusterID]map[string][]string `json:"services"`
}

const accessStatusPath = "/access/status"

// accessStatusTimeoutMs is the timeout for the status query, long enough for
// the server-side aggregation against clustermgr while bounding a hung node.
const accessStatusTimeoutMs = 10 * 1000

func showStatus(c *grumble.Context) error {
	addrs, err := resolveAccessAddrs(c.Flags.String("host"))
	if err != nil {
		return fmt.Errorf("resolve access addr: %w", err)
	}
	url := joinURL(addrs[0], accessStatusPath)

	cli := rpc.NewClient(&rpc.Config{ClientTimeoutMs: accessStatusTimeoutMs})

	ctx, cancel := context.WithTimeout(common.CmdContext(), time.Duration(accessStatusTimeoutMs)*time.Millisecond)
	defer cancel()

	st := &accessStatus{}
	if err := cli.GetWith(ctx, url, st); err != nil {
		return fmt.Errorf("get access status: %w", err)
	}
	fmt.Println(common.Readable(st))
	return nil
}

// resolveAccessAddrs resolves the target access service addresses.
// --host overrides the config; otherwise PriorityAddrs then consul discovery.
func resolveAccessAddrs(host string) ([]string, error) {
	if host != "" {
		return []string{normalizeAddr(host)}, nil
	}
	if addrs := config.AccessPriorityAddrs(); len(addrs) > 0 {
		return addrs, nil
	}
	if consul := config.AccessConsulAddr(); consul != "" {
		return discoverAccessServices(consul)
	}
	return nil, fmt.Errorf("no access host configured, set --host or priority_addrs/consul_addr")
}

// discoverAccessServices discovers access service hosts from consul, mirroring
// the discovery logic of the root access command.
func discoverAccessServices(consulAddr string) ([]string, error) {
	cli, err := config.NewConsulClient(consulAddr)
	if err != nil {
		return nil, err
	}
	services, _, err := cli.Health().Service("access", "", true,
		&api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, len(services))
	for _, service := range services {
		address := service.Service.Address
		if address == "" {
			address = service.Node.Address
		}
		addrs = append(addrs, fmt.Sprintf("http://%s:%d", address, service.Service.Port))
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no access service discovered on consul: %s", consulAddr)
	}
	return addrs, nil
}

// normalizeAddr completes the scheme for a bare host:port input, following the
// convention of other cli commands.
func normalizeAddr(addr string) string {
	if addr == "" {
		return addr
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return "http://" + addr
	}
	return addr
}

// joinURL joins an address (with scheme) and a path into a full URL.
func joinURL(addr, path string) string {
	return strings.TrimRight(addr, "/") + "/" + strings.TrimLeft(path, "/")
}

func addCmdStatus(cmd *grumble.Command) {
	statusCommand := &grumble.Command{
		Name:     "status",
		Help:     "show access status",
		LongHelp: "GET /access/status from access service and print cluster runtime snapshot",
		Run:      showStatus,
		Flags: func(f *grumble.Flags) {
			flags.VerboseRegister(f)
			f.StringL("host", "", "target access service ip:port, empty uses config addresses")
		},
	}
	cmd.AddCommand(statusCommand)
}
