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

import (
	"context"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"
)

type ossMetaLoader interface {
	loadPolicy(context.Context) (*Policy, error)
	loadACL(context.Context) (*AccessControlPolicy, error)
	loadCORS(context.Context) (*CORSConfiguration, error)
	loadObjectLock(context.Context) (*ObjectLockConfig, error)
	storePolicy(*Policy)
	storeACL(*AccessControlPolicy)
	storeCORS(*CORSConfiguration)
	storeObjectLock(*ObjectLockConfig)
	setSynced()
}

type strictMetaLoader struct {
	v *Volume
}

type cacheMetaLoader struct {
	om     *OSSMeta
	sml    *strictMetaLoader
	sf     singleflight.Group
	synced *int32
}

// OSSMeta is bucket policy and ACL metadata.
type OSSMeta struct {
	policy     *Policy
	acl        *AccessControlPolicy
	corsConfig *CORSConfiguration
	lockConfig *ObjectLockConfig
	policyLock sync.RWMutex
	aclLock    sync.RWMutex
	corsLock   sync.RWMutex
	objectLock sync.RWMutex
}

func (c *cacheMetaLoader) loadPolicy(ctx context.Context) (*Policy, error) {
	c.om.policyLock.RLock()
	p := c.om.policy
	c.om.policyLock.RUnlock()
	if p == nil && atomic.LoadInt32(c.synced) == 0 {
		ret, err, _ := c.sf.Do(XAttrKeyOSSPolicy, func() (interface{}, error) {
			return c.sml.loadPolicy(ctx)
		})
		if err != nil {
			return nil, err
		}
		p = ret.(*Policy)
		c.storePolicy(p)
	}

	return p, nil
}

func (c *cacheMetaLoader) storePolicy(p *Policy) {
	c.om.policyLock.Lock()
	c.om.policy = p
	c.om.policyLock.Unlock()
	return
}

func (c *cacheMetaLoader) loadACL(ctx context.Context) (*AccessControlPolicy, error) {
	c.om.aclLock.RLock()
	acl := c.om.acl
	c.om.aclLock.RUnlock()
	if acl == nil && atomic.LoadInt32(c.synced) == 0 {
		ret, err, _ := c.sf.Do(XAttrKeyOSSACL, func() (interface{}, error) {
			return c.sml.loadACL(ctx)
		})
		if err != nil {
			return nil, err
		}
		acl = ret.(*AccessControlPolicy)
		c.storeACL(acl)
	}

	return acl, nil
}

func (c *cacheMetaLoader) storeACL(p *AccessControlPolicy) {
	c.om.aclLock.Lock()
	c.om.acl = p
	c.om.aclLock.Unlock()
	return
}

func (c *cacheMetaLoader) loadCORS(ctx context.Context) (*CORSConfiguration, error) {
	c.om.corsLock.RLock()
	cors := c.om.corsConfig
	c.om.corsLock.RUnlock()
	if cors == nil && atomic.LoadInt32(c.synced) == 0 {
		ret, err, _ := c.sf.Do(XAttrKeyOSSCORS, func() (interface{}, error) {
			return c.sml.loadCORS(ctx)
		})
		if err != nil {
			return nil, err
		}
		cors = ret.(*CORSConfiguration)
		c.storeCORS(cors)
	}

	return cors, nil
}

func (c *cacheMetaLoader) storeCORS(cors *CORSConfiguration) {
	c.om.corsLock.Lock()
	c.om.corsConfig = cors
	c.om.corsLock.Unlock()
	return
}

func (c *cacheMetaLoader) loadObjectLock(ctx context.Context) (*ObjectLockConfig, error) {
	c.om.objectLock.RLock()
	config := c.om.lockConfig
	c.om.objectLock.RUnlock()
	if config == nil && atomic.LoadInt32(c.synced) == 0 {
		ret, err, _ := c.sf.Do(XAttrKeyOSSLock, func() (interface{}, error) {
			return c.sml.loadObjectLock(ctx)
		})
		if err != nil {
			return nil, err
		}
		config = ret.(*ObjectLockConfig)
		c.storeObjectLock(config)
	}

	return config, nil
}

func (c *cacheMetaLoader) storeObjectLock(config *ObjectLockConfig) {
	c.om.objectLock.Lock()
	c.om.lockConfig = config
	c.om.objectLock.Unlock()
	return
}

func (c *cacheMetaLoader) setSynced() {
	atomic.StoreInt32(c.synced, 1)
}

func (s *strictMetaLoader) loadPolicy(ctx context.Context) (*Policy, error) {
	return s.v.loadBucketPolicy(ctx)
}

func (s *strictMetaLoader) storePolicy(p *Policy) {
	// do nothing
}

func (s *strictMetaLoader) loadACL(ctx context.Context) (*AccessControlPolicy, error) {
	return s.v.loadBucketACL(ctx)
}

func (s *strictMetaLoader) storeACL(p *AccessControlPolicy) {
	// do nothing
}

func (s *strictMetaLoader) loadCORS(ctx context.Context) (*CORSConfiguration, error) {
	return s.v.loadBucketCors(ctx)
}

func (s *strictMetaLoader) storeCORS(cors *CORSConfiguration) {
	// do nothing
}

func (s *strictMetaLoader) loadObjectLock(ctx context.Context) (*ObjectLockConfig, error) {
	return s.v.loadObjectLock(ctx)
}

func (s *strictMetaLoader) storeObjectLock(cors *ObjectLockConfig) {
	// do nothing
}

func (s *strictMetaLoader) setSynced() {
	// do nothing
}
