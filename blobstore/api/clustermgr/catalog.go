package clustermgr

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"
)

const (
	hashBytesLength       = 16
	authVersionV1   uint8 = 1
)

func (c *Client) CreateSpace(ctx context.Context, args *CreateSpaceArgs) (err error) {
	err = c.PostWith(ctx, "/space/create", nil, args)
	return
}

func (c *Client) GetSpaceByName(ctx context.Context, args *GetSpaceByNameArgs) (ret *Space, err error) {
	ret = &Space{}
	err = c.GetWith(ctx, "/space/get?name="+args.Name, ret)
	return
}

func (c *Client) GetSpaceByID(ctx context.Context, args *GetSpaceByIDArgs) (ret *Space, err error) {
	ret = &Space{}
	err = c.GetWith(ctx, "/space/get?space_id="+args.SpaceID.ToString(), ret)
	return
}

func (c *Client) AuthSpace(ctx context.Context, args *AuthSpaceArgs) (err error) {
	err = c.GetWith(ctx, fmt.Sprintf("/space/auth?name=%s&token=%s", args.Name, args.Token), nil)
	return
}

func (c *Client) ListSpace(ctx context.Context, args *ListSpaceArgs) (ret ListSpaceRet, err error) {
	err = c.GetWith(ctx, fmt.Sprintf("/space/list?marker=%d&count=%d", args.Marker, args.Count), &ret)
	return
}

type AuthInfo struct {
	AccessKey string
	SecretKey string
}

// EncodeAuthInfo SDK generates token based on ak/sk
func EncodeAuthInfo(auth *AuthInfo) (token string, err error) {
	timeStamp := time.Now().Unix()
	hashBytes := CalculateHash(auth, timeStamp)

	w := bytes.NewBuffer([]byte{})
	if err = binary.Write(w, binary.LittleEndian, authVersionV1); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, &timeStamp); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, &hashBytes); err != nil {
		return
	}
	return base64.URLEncoding.EncodeToString(w.Bytes()), nil
}

// DecodeAuthInfo server parses token
func DecodeAuthInfo(token string) (timestamp int64, hashBytes []byte, err error) {
	b, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return
	}

	var authVersion uint8
	hashBytes = make([]byte, hashBytesLength)
	r := bytes.NewBuffer(b)
	if err = binary.Read(r, binary.LittleEndian, &authVersion); err != nil {
		return
	}
	if authVersion == authVersionV1 {
		if err = binary.Read(r, binary.LittleEndian, &timestamp); err != nil {
			return
		}
		if err = binary.Read(r, binary.LittleEndian, &hashBytes); err != nil {
			return
		}
	}
	return
}

// CalculateHash server caculates hash based on ak/sk and timeStamp
func CalculateHash(auth *AuthInfo, timeStamp int64) (hashBytes []byte) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(timeStamp))
	hash := md5.New()
	hash.Write(b)
	hash.Write([]byte(auth.AccessKey))
	hash.Write([]byte(auth.SecretKey))
	hashBytes = hash.Sum(nil)
	return hashBytes
}

func (c *Client) GetCatalogChanges(ctx context.Context, args *GetCatalogChangesArgs) (ret *GetCatalogChangesRet, err error) {
	ret = &GetCatalogChangesRet{}
	err = c.GetWith(ctx, fmt.Sprintf("/catalogchanges/get?route_version=%d&node_id=%d", args.RouteVersion, args.NodeID), ret)
	return
}
