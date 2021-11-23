package master

import (
	"encoding/json"
	"net/http"
	"reflect"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

type AuthHandle int

const (
	AuthKeepItem AuthHandle = iota
	AuthRemoveItem
	AuthModifyItem
)

type AuthPermission int

const (
	AuthAccessorPermission = iota // lowest permission
	AuthOwnerPermission
	AuthRootPermission // highest permission
)

type FilterFunc func(signs []*proto.AuthSignature, item interface{}) AuthHandle
type ModifyFunc func(item interface{}) interface{}

func (m *Server) parseSignatures(r *http.Request) (signs []*proto.AuthSignature, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	signature := r.FormValue(signatureKey)
	if signature == "" {
		return
	}

	if err = json.Unmarshal([]byte(signature), &signs); err != nil {
		return
	}
	return
}

func (m *Server) verifySignatures(
	signs []*proto.AuthSignature, path string,
	param string, permReq AuthPermission,
) (err error) {
	var (
		verifySign *proto.AuthSignature
		userInfo   *proto.UserInfo
		hasRoot    bool
	)

	if len(signs) == 0 {
		log.LogErrorf("Path(%s) verify signature fail: signs is empty", path)
		return proto.ErrNoPermission
	}

	for _, sign := range signs {
		if userInfo, err = m.user.getUserInfo(sign.UserID); err != nil {
			return err
		}

		authUser := &proto.AuthUser{userInfo.UserID, userInfo.AccessKey, userInfo.SecretKey}

		if verifySign, err = authUser.GenerateSignature(path); err != nil {
			return err
		}

		if err = verifySign.Compare(sign); err != nil {
			log.LogErrorf("Path(%s) verify signature of user %v fail: %v",
				path, sign.UserID, err)
			return proto.ErrNoPermission
		}

		if sign.UserID == "root" {
			sign.SetRoot()
			hasRoot = true
		}
	}

	switch permReq {
	case AuthRootPermission:
		if !hasRoot {
			log.LogErrorf("Path(%s) requires root permission", path)
			return proto.ErrNoPermission
		}
	case AuthOwnerPermission:
		var vol *Vol
		// param is volName that wants to be accessed
		if vol, err = m.cluster.getVol(param); err != nil {
			log.LogErrorf("Path(%s) cannot get volume of vol(%s)", path, param)
			return err
		}
		for _, sign := range signs {
			if sign.UserID == vol.Owner || sign.IsRoot() {
				return
			}
			log.LogErrorf("Path(%s) requires owner permission", path)
			return proto.ErrNoPermission
		}
	case AuthAccessorPermission:
		if path == proto.AdminCreateVol {
			// param is vol owner which should be one of signatures
			for _, sign := range signs {
				if sign.UserID == param || sign.IsRoot() {
					return
				}
			}
			log.LogErrorf("Path(%s) can only create vol for your own", path)
			return proto.ErrNoPermission
		}
	default:
		log.LogErrorf("Path(%s) invalid permission request(%v)", path, permReq)
		return proto.ErrParamError
	}

	return
}

func handleItem(
	signs []*proto.AuthSignature,
	filter FilterFunc, modify ModifyFunc,
	item reflect.Value,
) (reflect.Value, error) {
	var err error

	if !item.IsValid() {
		err = errors.New("[handleItem] invalid item")
		return reflect.ValueOf(nil), err
	}

	if filter == nil {
		return item, nil
	}

	how := filter(signs, item.Interface())
	switch how {
	case AuthRemoveItem:
		// return an invalid Value
		return reflect.Zero(item.Type()), nil
	case AuthModifyItem:
		if modify != nil {
			ret := modify(item.Interface())
			if ret == item.Interface() {
				panic("ModifyFunc must modify a new copy!")
			}
			item = reflect.ValueOf(ret)
		} else {
			err = errors.New("[handleItem] item needs modified but no modify function specified")
		}
	case AuthKeepItem:
		// nothing to do
	default:
		err = errors.NewErrorf("[handleItem] invalid return value %v from filter", how)
		return reflect.Zero(item.Type()), err
	}

	return item, err
}

func handleSlice(
	signs []*proto.AuthSignature,
	filter FilterFunc, modify ModifyFunc,
	data interface{},
) (retVal interface{}, err error) {
	dataType := reflect.TypeOf(data)
	newSlice := reflect.MakeSlice(dataType, 0, 0)
	set := reflect.ValueOf(data)
	for i := 0; i < set.Len(); i++ {
		var ret reflect.Value

		item := set.Index(i)
		if ret, err = handleItem(signs, filter, modify, item); err != nil {
			return
		}
		if ret.IsNil() {
			continue
		}
		newSlice = reflect.Append(newSlice, ret)
	}

	retVal = newSlice.Interface()
	return
}

func handleElement(
	signs []*proto.AuthSignature,
	filter FilterFunc, modify ModifyFunc,
	data interface{},
) (interface{}, error) {
	var (
		ret reflect.Value
		err error
	)

	item := reflect.ValueOf(data)
	if ret, err = handleItem(signs, filter, modify, item); err != nil {
		return ret.Interface(), proto.ErrNoPermission
	}
	if ret.IsNil() {
		return ret.Interface(), proto.ErrNoPermission
	}
	return ret.Interface(), nil
}

func (m *Server) filterBySignatures(
	signs []*proto.AuthSignature,
	filter FilterFunc, modify ModifyFunc,
	data interface{},
) (interface{}, error) {
	switch reflect.TypeOf(data).Kind() {
	case reflect.Slice:
		return handleSlice(signs, filter, modify, data)
	case reflect.Ptr:
		return handleElement(signs, filter, modify, data)
	default:
		err := errors.NewErrorf("[filterBySignatures] unsupported type %v", reflect.TypeOf(data))
		return nil, err
	}
	return nil, nil
}
