// Copyright 2019 The CubeFS Authors.
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

package objectnode

import "sync"

type ossMetaLoader interface {
	loadPolicy() (p *Policy, err error)
	loadACL() (p *AccessControlPolicy, err error)
	loadCors() (cors *CORSConfiguration, err error)
	storePolicy(p *Policy)
	storeACL(p *AccessControlPolicy)
	storeCors(cors *CORSConfiguration)
}

type strictMetaLoader struct {
	v *Volume
}

type cacheMetaLoader struct {
	om *OSSMeta
}

// OSSMeta is bucket policy and ACL metadata.
type OSSMeta struct {
	policy     *Policy
	acl        *AccessControlPolicy
	corsConfig *CORSConfiguration
	policyLock sync.RWMutex
	aclLock    sync.RWMutex
	corsLock   sync.RWMutex
}

func (c *cacheMetaLoader) loadPolicy() (p *Policy, err error) {
	c.om.policyLock.RLock()
	p = c.om.policy
	c.om.policyLock.RUnlock()
	return
}

func (c *cacheMetaLoader) storePolicy(p *Policy) {
	c.om.policyLock.Lock()
	c.om.policy = p
	c.om.policyLock.Unlock()
	return
}

func (c *cacheMetaLoader) loadACL() (p *AccessControlPolicy, err error) {
	c.om.aclLock.RLock()
	p = c.om.acl
	c.om.aclLock.RUnlock()
	return
}

func (c *cacheMetaLoader) storeACL(p *AccessControlPolicy) {
	c.om.aclLock.Lock()
	c.om.acl = p
	c.om.aclLock.Unlock()
	return
}

func (c *cacheMetaLoader) loadCors() (cors *CORSConfiguration, err error) {
	c.om.corsLock.RLock()
	cors = c.om.corsConfig
	c.om.corsLock.RUnlock()
	return
}

func (c *cacheMetaLoader) storeCors(cors *CORSConfiguration) {
	c.om.corsLock.Lock()
	c.om.corsConfig = cors
	c.om.corsLock.Unlock()
	return
}

func (s *strictMetaLoader) loadPolicy() (p *Policy, err error) {
	return s.v.loadBucketPolicy()
}

func (s *strictMetaLoader) storePolicy(p *Policy) {}

func (s *strictMetaLoader) loadACL() (acp *AccessControlPolicy, err error) {
	return s.v.loadBucketACL()
}

func (s *strictMetaLoader) storeACL(p *AccessControlPolicy) {}

func (s *strictMetaLoader) loadCors() (cors *CORSConfiguration, err error) {
	return s.v.loadBucketCors()
}

func (s *strictMetaLoader) storeCors(cors *CORSConfiguration) {}
