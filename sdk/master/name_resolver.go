// Copyright 2018 The CubeFS Authors.
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

package master

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"math/rand"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var domainRegexp = regexp.MustCompile(`^(?i)[a-z0-9-]+(\.[a-z0-9-]+)+\.?$`)

func IsValidDomain(domain string) bool {
	return domainRegexp.MatchString(domain)
}

type IpCache struct {
	sync.RWMutex
	Ts  int64 //time.Now().Unix()
	Ips []string
}

func (ic *IpCache) SetIps(ips []string) {
	ic.Lock()
	defer ic.Unlock()
	ic.Ips = ips
	ic.Ts = time.Now().Unix()
}

func (ic *IpCache) UpdateTs() {
	ic.Lock()
	defer ic.Unlock()
	ic.Ts = time.Now().Unix()
}

func (ic *IpCache) GetRandomIp() (ip string, err error) {
	ic.RLock()
	defer ic.RUnlock()
	if len(ic.Ips) == 0 {
		return "", fmt.Errorf("ip cache is empty")
	}
	randIndex := rand.Intn(len(ic.Ips))
	return ic.Ips[randIndex], nil
}

func (ic *IpCache) GetAllIps() (ips []string, err error) {
	ic.RLock()
	defer ic.RUnlock()
	if len(ic.Ips) == 0 {
		return nil, fmt.Errorf("ip cache is empty")
	}
	return ic.Ips, nil
}

type NameResolver struct {
	domains []string
	ips     []string
	port    uint64
	ic      *IpCache
}

// NewNameResolver parse raw master address configuration
// string and returns a new NameResolver instance.
// Notes that a valid format raw string member of addrs must match: "IP:PORT" or "DOMAIN:PORT"
// and PORT must be the same
func NewNameResolver(addrPorts []string) (ns *NameResolver, err error) {
	if len(addrPorts) == 0 {
		log.LogErrorf("NameResolver: empty addresses for name resolver")
		return nil, fmt.Errorf("empty addresses for name resolver")
	}
	var domains []string
	var ips []string

	port := uint64(0)
	for _, ap := range addrPorts {
		if ap == "" {
			continue
		}
		arr := strings.Split(ap, ":")
		/*if len(arr) != 2 {
			return nil, fmt.Errorf("wrong addr format [%v]", ap)
		}*/

		arrNum := len(arr)
		p := uint64(0)
		if arrNum == 2 {
			p, err = strconv.ParseUint(arr[1], 10, 64)
			if err != nil {
				log.LogErrorf("NameResolver: wrong addr format [%v]", ap)
				return nil, fmt.Errorf("wrong addr format [%v]", ap)
			}

		} else if arrNum == 1 {
			p = 80
		} else {
			log.LogErrorf("NameResolver: wrong addr format [%v]", ap)
			return nil, fmt.Errorf("wrong addr format [%v]", ap)
		}

		if port == 0 {
			port = p
		} else if port != p {
			log.LogErrorf("NameResolver: ports are not the same")
			return nil, fmt.Errorf("ports are not the same")
		}

		addr := net.ParseIP(arr[0])
		if addr == nil {
			if IsValidDomain(arr[0]) {
				domains = append(domains, arr[0])
			} else {
				log.LogErrorf("NameResolver: wrong addr format [%v]", ap)
				return nil, fmt.Errorf("wrong addr format [%v]", ap)
			}
		} else {
			ips = append(ips, addr.String())
		}
	}
	ic := &IpCache{}

	ns = &NameResolver{
		domains: domains,
		ips:     ips,
		port:    port,
		ic:      ic,
	}
	log.LogDebugf("NameResolver: add ip[%v], domain[%v], port[%v]", ips, domains, port)
	return ns, nil
}

func (ns *NameResolver) GetRandomIp() (ip string, err error) {
	return ns.ic.GetRandomIp()
}

func (ns *NameResolver) GetAllIps() (ips []string, err error) {
	return ns.ic.GetAllIps()
}

func (ns *NameResolver) GetAllAddresses() (addrs []string, err error) {
	ips, err := ns.ic.GetAllIps()
	if err != nil {
		return nil, err
	}

	for _, ip := range ips {
		addr := fmt.Sprintf("%s:%d", ip, ns.port)
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

func (ns *NameResolver) isChanged(ipSet map[string]struct{}) (changed bool) {
	for _, ip := range ns.ic.Ips {
		if _, ok := ipSet[ip]; !ok {
			changed = true
		}
	}

	if !changed {
		if len(ipSet) != len(ns.ic.Ips) {
			changed = true
		}
	}
	return
}

func (ns *NameResolver) Resolve() (changed bool, err error) {
	if len(ns.ips) == 0 && len(ns.domains) == 0 {
		return false, fmt.Errorf("name or ip empty")
	}

	ipSet := make(map[string]struct{}, 0)

	if len(ns.domains) > 0 {
		var addrs []net.IP
		for _, domain := range ns.domains {
			addrs, err = net.LookupIP(domain)
			if err != nil {
				log.LogWarnf("domain [%v] resolved failed", domain)
				continue
			} else {
				for _, ip := range addrs {
					ipSet[ip.String()] = struct{}{}
				}
			}
		}
	}

	for _, ip := range ns.ips {
		ipSet[ip] = struct{}{}
	}

	if len(ipSet) == 0 {
		return false, errors.New("resolve: resolving result is empty")
	}

	var ips []string
	for ip := range ipSet {
		ips = append(ips, ip)
	}
	changed = ns.isChanged(ipSet)
	if changed {
		log.LogInfof("Resolve: resolving result is changed from %v to %v", ns.ic.Ips, ips)
		ns.ic.SetIps(ips)
	} else {
		log.LogDebugf("Resolve: resolving result is not changed %v", ns.ic.Ips)
	}

	ns.ic.UpdateTs()

	return changed, nil
}
