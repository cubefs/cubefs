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

package route

import (
	"fmt"

	"github.com/cubefs/cubefs/util/lib"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	UserRouteCol = "user_route"
	VolCol       = "vol_tbl"
)

var mgoSeesion *mgo.Session

func initDB(addr string) (err error) {
	session, err := mgo.Dial(addr)
	if err != nil {
		return fmt.Errorf("init mgo seesion failed, addr %s, err %s", addr, err.Error())
	}

	session.SetMode(mgo.Strong, true)
	mgoSeesion = session

	err = mgoSeesion.DB("").C(UserRouteCol).EnsureIndex(mgo.Index{Key: []string{"user_id", "app_id"}, Background: true, Unique: true})
	if err != nil {
		return fmt.Errorf("ensure index failed, table %s, err %s", UserRouteCol, err.Error())
	}

	if err = mgoSeesion.DB("").C(VolCol).EnsureIndex(mgo.Index{Key: []string{"vol_id", "group_id"}, Background: true, Unique: true}); err != nil {
		return fmt.Errorf("ensuer %s index failed, err %s", VolCol, err.Error())
	}

	return nil
}

type UserRoute struct {
	UserId   string `bson:"user_id"`
	AppId    uint32 `bson:"app_id"`
	GroupId  uint32 `bson:"group_id"`
	VolumeId uint32 `bson:"vol_id"`
	HashId   uint32 `bson:"hash_id"`
	Status   uint8  `bson:"status"`
}

type UserRouterApi interface {
	AddRoute(r *UserRoute) error
	GetRoute(uid string, appId int) (u *UserRoute, err error)
	ListRouteByUid(uid string) (us []UserRoute, err error)
	UpdateRoute(r *UserRoute) error
}

type UserRouteImp struct {
}

func (u *UserRouteImp) UpdateRoute(r *UserRoute) (err error) {
	session := mgoSeesion.Copy()
	defer session.Close()

	sel := bson.M{
		"app_id":  r.AppId,
		"user_id": r.UserId,
	}

	update := bson.M{
		"group_id": r.GroupId,
		"vol_id":   r.VolumeId,
		"hash_id":  r.HashId,
		"status":   r.Status,
	}

	err = session.DB("").C(UserRouteCol).Update(sel, bson.M{"$set": update})
	if err != nil {
		return
	}

	return
}

func (u *UserRouteImp) AddRoute(r *UserRoute) (err error) {
	session := mgoSeesion.Copy()
	defer session.Close()
	col := session.DB("").C(UserRouteCol)

	r.Status = lib.RouteNormalStatus

	err = col.Insert(r)
	if err != nil {
		if mgo.IsDup(err) {
			return lib.ERR_ROUTE_EXIST.With(err)
		}
		return err
	}

	return nil
}

func (u *UserRouteImp) GetRoute(uid string, appId int) (ur *UserRoute, err error) {
	query := bson.M{}
	query["user_id"] = uid
	query["app_id"] = appId

	session := mgoSeesion.Copy()
	defer session.Close()

	col := session.DB("").C(UserRouteCol)
	if err := col.Find(query).One(&ur); err != nil {
		if err == mgo.ErrNotFound {
			return nil, lib.ERR_ROUTE_NOT_EXIST.With(err)
		}
		return nil, err
	}

	return ur, nil
}

func (u *UserRouteImp) ListRouteByUid(uid string) (us []UserRoute, err error) {
	query := bson.M{}
	query["user_id"] = uid

	session := mgoSeesion.Copy()
	defer session.Close()

	col := session.DB("").C(UserRouteCol)
	if err := col.Find(query).All(&us); err != nil {
		if err == mgo.ErrNotFound {
			return nil, lib.ERR_ROUTE_NOT_EXIST.With(err)
		}
		return nil, err
	}

	return us, nil
}

type VolItem struct {
	GroupId  uint32 `bson:"group_id"`
	VolumeId uint32 `bson:"vol_id"`
	Ak       string `bson:"ak"`
	Sk       string `bson:"sk"`
}

type VolOpApi interface {
	AddVol(v *VolItem) error
	GetVol(gid, vid uint32) (e *VolItem, err error)
	ListVols() (arr []VolItem, err error)
}

type VolItemImp struct {
}

func (v *VolItemImp) AddVol(vi *VolItem) (err error) {
	session := mgoSeesion.Copy()
	defer session.Close()
	col := session.DB("").C(VolCol)
	err = col.Insert(vi)
	if mgo.IsDup(err) {
		return lib.ERR_ROUTE_EXIST.With(err)
	}
	return err
}

func (v *VolItemImp) GetVol(gid, vid uint32) (e *VolItem, err error) {
	query := bson.M{}
	query["vol_id"] = vid
	query["group_id"] = gid

	session := mgoSeesion.Copy()
	defer session.Close()

	col := session.DB("").C(VolCol)
	if err := col.Find(query).One(&e); err != nil {
		if err == mgo.ErrNotFound {
			return nil, lib.ERR_ROUTE_NOT_EXIST.With(err)
		}
		return nil, err
	}

	return
}

func (v *VolItemImp) ListVols() (arr []VolItem, err error) {
	query := bson.M{}

	session := mgoSeesion.Copy()
	defer session.Close()

	col := session.DB("").C(VolCol)
	if err := col.Find(query).All(&arr); err != nil {
		return nil, err
	}

	return
}
