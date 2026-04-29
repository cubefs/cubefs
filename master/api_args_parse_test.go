// Copyright 2025 The CubeFS Authors.
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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

// copyVolForTest returns a shallow copy of v for isolated test mutations.
// Vol embeds sync.RWMutex through TopoSubItem; this is safe in tests where
// the mutex is guaranteed to be unlocked at the point of copy.
func copyVolForTest(v *Vol) Vol {
	return *v //nolint:govet
}

func apiArgsNewGet(t *testing.T, rawQuery string) *http.Request {
	t.Helper()
	u := "http://127.0.0.1/admin?"
	if rawQuery != "" {
		u += rawQuery
	}
	return httptest.NewRequest(http.MethodGet, u, nil)
}

func apiArgsNewPostForm(t *testing.T, form url.Values) *http.Request {
	t.Helper()
	body := form.Encode()
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/admin", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req
}

func apiArgsNewJSONBody(t *testing.T, v any) *http.Request {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/admin", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestParseRequestForRaftNode(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		r := apiArgsNewGet(t, "id=2&addr=127.0.0.1:9090")
		id, host, err := parseRequestForRaftNode(r)
		require.NoError(t, err)
		require.Equal(t, uint64(2), id)
		require.Equal(t, "127.0.0.1:9090", host)
	})
	t.Run("missing_addr", func(t *testing.T) {
		_, _, err := parseRequestForRaftNode(apiArgsNewGet(t, "id=1"))
		require.Error(t, err)
	})
	t.Run("host_without_colon", func(t *testing.T) {
		_, _, err := parseRequestForRaftNode(apiArgsNewGet(t, "id=1&addr=nocolon"))
		require.Error(t, err)
	})
}

func TestExtractTxParams(t *testing.T) {
	t.Run("extractTxTimeout", func(t *testing.T) {
		v, err := extractTxTimeout(apiArgsNewGet(t, "txTimeout=5"), 1)
		require.NoError(t, err)
		require.Equal(t, int64(5), v)
		_, err = extractTxTimeout(apiArgsNewGet(t, "txTimeout=0"), 10)
		require.Error(t, err)
		_, err = extractTxTimeout(apiArgsNewGet(t, "txTimeout=9999"), 1)
		require.Error(t, err)
	})
	t.Run("extractTxConflictRetryNum", func(t *testing.T) {
		v, err := extractTxConflictRetryNum(apiArgsNewGet(t, "txConflictRetryNum=10"), 1)
		require.NoError(t, err)
		require.Equal(t, int64(10), v)
		_, err = extractTxConflictRetryNum(apiArgsNewGet(t, "txConflictRetryNum=0"), 1)
		require.Error(t, err)
	})
	t.Run("extractTxConflictRetryInterval", func(t *testing.T) {
		v, err := extractTxConflictRetryInterval(apiArgsNewGet(t, "txConflictRetryInterval=100"), 500)
		require.NoError(t, err)
		require.Equal(t, int64(100), v)
		_, err = extractTxConflictRetryInterval(apiArgsNewGet(t, "txConflictRetryInterval=1"), 500)
		require.Error(t, err)
	})
	t.Run("extractTxOpLimitInterval", func(t *testing.T) {
		v, err := extractTxOpLimitInterval(apiArgsNewGet(t, "txOpLimit=7"), 3)
		require.NoError(t, err)
		require.Equal(t, 7, v)
	})
	t.Run("hasTxParams", func(t *testing.T) {
		require.True(t, hasTxParams(apiArgsNewGet(t, "enableTxMask=off")))
		require.True(t, hasTxParams(apiArgsNewGet(t, "txTimeout=3")))
		require.False(t, hasTxParams(apiArgsNewGet(t, "name=abc")))
	})
	t.Run("parseTxMask", func(t *testing.T) {
		m, err := parseTxMask(apiArgsNewGet(t, ""), proto.TxOpMaskCreate)
		require.NoError(t, err)
		require.Equal(t, proto.TxOpMaskCreate, m)

		m, err = parseTxMask(apiArgsNewGet(t, "enableTxMask=create&txForceReset=true"), proto.TxOpMaskMkdir)
		require.NoError(t, err)
		require.Equal(t, proto.TxOpMaskCreate, m)

		_, err = parseTxMask(apiArgsNewGet(t, "enableTxMask=off"), proto.TxOpMaskMkdir)
		require.NoError(t, err)

		m, err = parseTxMask(apiArgsNewGet(t, "enableTxMask=create|mkdir"), proto.TxOpMaskRename)
		require.NoError(t, err)
		require.NotEqual(t, proto.TxOpMask(0), m)

		_, err = parseTxMask(apiArgsNewGet(t, "enableTxMask=not-a-mask"), proto.TxOpMaskOff)
		require.Error(t, err)
	})
}

func TestParseRequestForAddNode(t *testing.T) {
	q := "addr=10.0.0.1:17320&zoneName=z1&rack=r1&heartbeatPort=5901&replicaPort=5902&mediaType=1&poolId=2"
	addr, hb, rp, zn, rack, mt, pool, err := parseRequestForAddNode(apiArgsNewGet(t, q))
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:17320", addr)
	require.Equal(t, "5901", hb)
	require.Equal(t, "5902", rp)
	require.Equal(t, "z1", zn)
	require.Equal(t, "r1", rack)
	require.Equal(t, uint32(1), mt)
	require.Equal(t, uint8(2), pool)

	_, _, _, _, _, _, _, err = parseRequestForAddNode(apiArgsNewGet(t, "addr=10.0.0.1:17320&poolId=abc"))
	require.Error(t, err)
}

func TestParseDecomNodeReqs(t *testing.T) {
	r := apiArgsNewGet(t, "addr=10.0.0.1:17320&count=5")
	addr, lim, err := parseDecomNodeReq(r)
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:17320", addr)
	require.Equal(t, 5, lim)

	_, lim, err = parseDecomDataNodeReq(r)
	require.NoError(t, err)
	require.Equal(t, 5, lim)

	_, err = parseAndExtractNodeAddr(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestParseRequestToGetTaskResponse(t *testing.T) {
	task := &proto.AdminTask{ID: "tid", OpCode: 3}
	body, err := json.Marshal(task)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	out, err := parseRequestToGetTaskResponse(req)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, "tid", out.ID)
}

func TestParseVolNameAndGetVolParameter(t *testing.T) {
	name, err := parseVolName(apiArgsNewGet(t, "name=myvol01"))
	require.NoError(t, err)
	require.Equal(t, "myvol01", name)

	_, err = parseVolName(apiArgsNewGet(t, "name="))
	require.Error(t, err)

	req := apiArgsNewGet(t, "name=myvol02&authKey=secret")
	p, err := parseGetVolParameter(req)
	require.NoError(t, err)
	require.Equal(t, "myvol02", p.name)
	require.Equal(t, "secret", p.authKey)

	req2, err := http.NewRequest(http.MethodGet, "http://x/", nil)
	require.NoError(t, err)
	req2.Header.Set(proto.SkipOwnerValidation, "true")
	q := req2.URL.Query()
	q.Set("name", "myvol03")
	req2.URL.RawQuery = q.Encode()
	p2, err := parseGetVolParameter(req2)
	require.NoError(t, err)
	require.True(t, p2.skipOwnerValidation)
}

func TestParseVolVerStrategy(t *testing.T) {
	s, force, err := parseVolVerStrategy(apiArgsNewGet(t, "enable=false&count=2&periodic=1&force=true"))
	require.NoError(t, err)
	require.False(t, s.Enable)
	require.Equal(t, 2, s.KeepVerCnt)
	require.Equal(t, 1, s.Periodic)
	require.True(t, force)
}

func TestParseRequestToDeleteVol(t *testing.T) {
	r := apiArgsNewGet(t, "name=myvol04&authKey=k&delete=false&forceDelVol=true")
	name, ak, status, force, err := parseRequestToDeleteVol(r)
	require.NoError(t, err)
	require.Equal(t, "myvol04", name)
	require.Equal(t, "k", ak)
	require.False(t, status)
	require.True(t, force)
}

func TestParseColdVolUpdateArgs(t *testing.T) {
	v := &Vol{Name: "cv", volStorageClass: proto.StorageClass_Replica_SSD, CacheSubItem: CacheSubItem{EbsBlkSize: 100}}
	args, err := parseColdVolUpdateArgs(apiArgsNewGet(t, "ebsBlkSize=200"), v)
	require.NoError(t, err)
	require.Equal(t, 200, args.objBlockSize)

	v2 := &Vol{Name: "cv2", volStorageClass: proto.StorageClass_BlobStore, CacheSubItem: CacheSubItem{EbsBlkSize: 64}}
	args2, err := parseColdVolUpdateArgs(apiArgsNewGet(t, ""), v2)
	require.NoError(t, err)
	require.Equal(t, 64, args2.objBlockSize)
}

func TestParseColdArgs(t *testing.T) {
	a, err := parseColdArgs(apiArgsNewGet(t, "ebsBlkSize=4096"))
	require.NoError(t, err)
	require.Equal(t, 4096, a.objBlockSize)

	_, err = parseColdArgs(apiArgsNewGet(t, "ebsBlkSize=badint"))
	require.Error(t, err)
}

func TestParseVolUpdateReq_minimal(t *testing.T) {
	v0, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)
	vol := copyVolForTest(v0)
	// commonVol may have empty allowedStorageClass; parseVolUpdateReq validates requested class against it.
	vol.allowedStorageClass = []uint32{vol.volStorageClass}
	req := &updateVolReq{}
	r := apiArgsNewGet(t, "txTimeout=1&txConflictRetryNum=100&txConflictRetryInterval=500")
	err = parseVolUpdateReq(r, &vol, req)
	require.NoError(t, err)
}

func TestParseRequestToCreateVol_minimal(t *testing.T) {
	name := fmt.Sprintf("vt%016x", uint64(time.Now().UnixNano()))
	form := url.Values{}
	form.Set(nameKey, name)
	form.Set(volOwnerKey, "cfs")
	form.Set(replicaNumKey, "3")
	form.Set(volCapacityKey, "100")
	form.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
	r := apiArgsNewPostForm(t, form)
	cv := &createVolReq{}
	err := parseRequestToCreateVol(r, cv, server)
	require.NoError(t, err)
	require.Equal(t, name, cv.name)
	require.Equal(t, "cfs", cv.owner)
	require.Equal(t, uint8(3), cv.dpReplicaNum)
}

func TestParseDataPartitionRequests(t *testing.T) {
	cnt, vol, pool, err := parseRequestToCreateDataPartition(apiArgsNewGet(t, "count=2&name=myvol05&poolId=1"))
	require.NoError(t, err)
	require.Equal(t, 2, cnt)
	require.Equal(t, "myvol05", vol)
	require.Equal(t, uint8(1), pool)

	_, _, _, err = parseRequestToCreateDataPartition(apiArgsNewGet(t, "name=x"))
	require.Error(t, err)

	id, vn, err := parseRequestToGetDataPartition(apiArgsNewGet(t, "id=99&name=vn"))
	require.NoError(t, err)
	require.Equal(t, uint64(99), id)
	require.Equal(t, "vn", vn)

	z, ns, err := parseRequestToBalanceMetaPartition(apiArgsNewGet(t, "zoneName=zz&nodesetId=7"))
	require.NoError(t, err)
	require.Equal(t, "zz", z)
	require.Equal(t, "7", ns)

	id, err = parseRequestToLoadDataPartition(apiArgsNewGet(t, "id=42"))
	require.NoError(t, err)
	require.Equal(t, uint64(42), id)
}

func TestParseMetaReplicaRequests(t *testing.T) {
	id, addr, err := parseRequestToAddMetaReplica(apiArgsNewGet(t, "id=1&addr=10.0.0.1:17210"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)
	require.Equal(t, "10.0.0.1:17210", addr)

	_, _, err = parseRequestToRemoveMetaReplica(apiArgsNewGet(t, "id=2&addr=10.0.0.2:17210"))
	require.NoError(t, err)

	_, _, err = parseRequestToAddMetaPartitionLearner(apiArgsNewGet(t, "id=3&addr=10.0.0.3:17210"))
	require.NoError(t, err)

	_, _, err = parseRequestToPromoteMetaReplica(apiArgsNewGet(t, "id=4&addr=10.0.0.4:17210"))
	require.NoError(t, err)
}

func TestParseDataReplicaAndDecommission(t *testing.T) {
	id, _, err := parseRequestToAddDataReplica(apiArgsNewGet(t, "id=5&addr=10.0.0.5:17320"))
	require.NoError(t, err)
	require.Equal(t, uint64(5), id)

	_, _, err = parseRequestToRemoveDataReplica(apiArgsNewGet(t, "id=6&addr=10.0.0.6:17320"))
	require.NoError(t, err)

	_, _, err = parseRequestToDecommissionDataPartition(apiArgsNewGet(t, "id=7&addr=10.0.0.7:17320"))
	require.NoError(t, err)
}

func TestParseMetaPartitionLoadDecom(t *testing.T) {
	pid, err := parseRequestToLoadMetaPartition(apiArgsNewGet(t, "id=11"))
	require.NoError(t, err)
	require.Equal(t, uint64(11), pid)

	pid, addr, err := parseRequestToDecommissionMetaPartition(apiArgsNewGet(t, "id=12&addr=10.0.0.8:17210"))
	require.NoError(t, err)
	require.Equal(t, uint64(12), pid)
	require.Equal(t, "10.0.0.8:17210", addr)
}

func TestExtractDiskPathAndDisable(t *testing.T) {
	p, err := extractDiskPath(apiArgsNewGet(t, "disk=/data/disk1"))
	require.NoError(t, err)
	require.Equal(t, "/data/disk1", p)

	_, err = extractDiskPath(apiArgsNewGet(t, ""))
	require.Error(t, err)

	d, err := extractDiskDisable(apiArgsNewGet(t, ""))
	require.NoError(t, err)
	require.True(t, d)

	d, err = extractDiskDisable(apiArgsNewGet(t, "diskDisable=false"))
	require.NoError(t, err)
	require.False(t, d)
}

func TestParseAndExtractStatusForbiddenDpRepair(t *testing.T) {
	st, err := parseAndExtractStatus(apiArgsNewGet(t, "enable=true"))
	require.NoError(t, err)
	require.True(t, st)

	fb, err := parseAndExtractForbidden(apiArgsNewGet(t, "forbidden=false"))
	require.NoError(t, err)
	require.False(t, fb)

	sz, err := parseAndExtractDpRepairBlockSize(apiArgsNewGet(t, "dpRepairBlockSize=65536"))
	require.NoError(t, err)
	require.Equal(t, uint64(65536), sz)
}

func TestExtractSelectorsAndFollowerRead(t *testing.T) {
	r := apiArgsNewGet(t, "dataNodesetSelector=sel1&metaNodesetSelector=sel2&dataNodeSelector=ds&metaNodeSelector=ms&followerRead=true")
	require.Equal(t, "sel1", extractDataNodesetSelector(r))
	require.Equal(t, "sel2", extractMetaNodesetSelector(r))
	require.Equal(t, "ds", extractDataNodeSelector(r))
	require.Equal(t, "ms", extractMetaNodeSelector(r))
	fr, ex, err := extractFollowerRead(r)
	require.NoError(t, err)
	require.True(t, ex)
	require.True(t, fr)
}

func TestParseAndExtractDirLimit(t *testing.T) {
	lim, err := parseAndExtractDirLimit(apiArgsNewGet(t, "dirSizeLimit=100"))
	require.NoError(t, err)
	require.Equal(t, uint32(100), lim)

	lim, err = parseAndExtractDirLimit(apiArgsNewGet(t, "dirQuota=200"))
	require.NoError(t, err)
	require.Equal(t, uint32(200), lim)

	_, err = parseAndExtractDirLimit(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestParseAndExtractThresholdsAndGOGC(t *testing.T) {
	th, err := parseAndExtractThreshold(apiArgsNewGet(t, "threshold=0.75"))
	require.NoError(t, err)
	require.InDelta(t, 0.75, th, 1e-9)

	h, err := parseAndExtractVolDeletionDelayTime(apiArgsNewGet(t, "volDeletionDelayTime=24"))
	require.NoError(t, err)
	require.Equal(t, 24, h)

	h, err = parseAndExtractFlashTopoDeletionDelayTime(apiArgsNewGet(t, "flashTopoDeletionDelayTime=12"))
	require.NoError(t, err)
	require.Equal(t, 12, h)

	gc, err := parseAndExtractMetaNodeGOGC(apiArgsNewGet(t, "metaNodeGOGC=150"))
	require.NoError(t, err)
	require.Equal(t, 150, gc)

	gc, err = parseAndExtractDataNodeGOGC(apiArgsNewGet(t, "dataNodeGOGC=200"))
	require.NoError(t, err)
	require.Equal(t, 200, gc)
}

func TestParseAndExtractFileStatsThresholds(t *testing.T) {
	arr, err := parseAndExtractFileStatsThresholds(apiArgsNewGet(t, "threshold=1,2,3"))
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, arr)

	_, err = parseAndExtractFileStatsThresholds(apiArgsNewGet(t, "threshold=bad"))
	require.Error(t, err)
}

func TestParseAndExtractSetNodeSetInfoParams(t *testing.T) {
	m, err := parseAndExtractSetNodeSetInfoParams(apiArgsNewGet(t, "count=10&zoneName=z2&id=3"))
	require.NoError(t, err)
	require.Equal(t, uint64(10), m[countKey])
	require.Equal(t, "z2", m[zoneNameKey])
	require.Equal(t, uint64(3), m[idKey])

	_, err = parseAndExtractSetNodeSetInfoParams(apiArgsNewGet(t, "count=1"))
	require.Error(t, err)
}

func TestParseAndExtractSetNodeInfoParams_minimal(t *testing.T) {
	m, err := parseAndExtractSetNodeInfoParams(apiArgsNewGet(t, "batchCount=5"))
	require.NoError(t, err)
	require.Equal(t, uint64(5), m[nodeDeleteBatchCountKey])

	_, err = parseAndExtractSetNodeInfoParams(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestValidateRequestToCreateMetaPartition(t *testing.T) {
	v, c, reg, err := validateRequestToCreateMetaPartition(apiArgsNewGet(t, "count=2&name=myvol06&region=r1"))
	require.NoError(t, err)
	require.Equal(t, "myvol06", v)
	require.Equal(t, 2, c)
	require.Equal(t, "r1", reg)

	_, _, _, err = validateRequestToCreateMetaPartition(apiArgsNewGet(t, "count=0&name=myvol07"))
	require.Error(t, err)
}

func TestParseRequestToUpdateMetaPartitionRegion(t *testing.T) {
	pid, reg, err := parseRequestToUpdateMetaPartitionRegion(apiArgsNewGet(t, "id=9&region=east-1"))
	require.NoError(t, err)
	require.Equal(t, uint64(9), pid)
	require.Equal(t, "east-1", reg)

	_, _, err = parseRequestToUpdateMetaPartitionRegion(apiArgsNewGet(t, "id=1&region="))
	require.Error(t, err)
}

func TestParseAndExtractPartitionInfo(t *testing.T) {
	pid, err := parseAndExtractPartitionInfo(apiArgsNewGet(t, "id=88"))
	require.NoError(t, err)
	require.Equal(t, uint64(88), pid)
}

func TestExtractAuthAndClientID(t *testing.T) {
	ak, err := extractAuthKey(apiArgsNewGet(t, "authKey=sekret"))
	require.NoError(t, err)
	require.Equal(t, "sekret", ak)

	ck, err := extractClientIDKey(apiArgsNewGet(t, "clientIDKey=cid"))
	require.NoError(t, err)
	require.Equal(t, "cid", ck)
}

func TestParseVolStatReq(t *testing.T) {
	n, ver, byMeta, err := parseVolStatReq(apiArgsNewGet(t, "name=myvol08&version=2&countByMeta=true"))
	require.NoError(t, err)
	require.Equal(t, "myvol08", n)
	require.Equal(t, 2, ver)
	require.True(t, byMeta)
}

func TestParseQosInfo(t *testing.T) {
	req := apiArgsNewJSONBody(t, map[string]any{"vol": "v1"})
	info, err := parseQosInfo(req)
	require.NoError(t, err)
	require.NotNil(t, info)
}

func TestParseAndExtractNameAndDecommissionType(t *testing.T) {
	n, err := parseAndExtractName(apiArgsNewGet(t, "name=myvol09"))
	require.NoError(t, err)
	require.Equal(t, "myvol09", n)

	dt, err := parseAndExtractDecommissionType(apiArgsNewGet(t, "decommissionType=2"))
	require.NoError(t, err)
	require.Equal(t, 2, dt)
}

func TestHTTPReplyHelpers(t *testing.T) {
	ok := newSuccessHTTPReply("data")
	require.Equal(t, int32(proto.ErrCodeSuccess), ok.Code)
	require.Equal(t, "data", ok.Data)

	er := newErrHTTPReply(nil)
	require.Equal(t, int32(proto.ErrCodeSuccess), er.Code)

	er = newErrHTTPReply(proto.ErrVolNotExists)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), er.Code)

	er = newErrHTTPReply(errors.New("custom unknown"))
	require.Equal(t, int32(proto.ErrCodeInternalError), er.Code)

	er = newErrHTTPReply(proto.ErrFollowerReadLeaseTimeRange)
	require.Equal(t, int32(proto.ErrCodeParamError), er.Code)

	er = newErrHTTPReply(errAutoMpMetaRepairNeedsLearnerDecommission)
	require.Equal(t, int32(proto.ErrCodeParamError), er.Code)

	rec := httptest.NewRecorder()
	req := apiArgsNewGet(t, "")
	require.NoError(t, sendOkReply(rec, req, newSuccessHTTPReply(map[string]int{"a": 1})))
	require.EqualValues(t, http.StatusOK, rec.Code)

	recGz := httptest.NewRecorder()
	reqGz := apiArgsNewGet(t, "")
	reqGz.Header.Set(proto.HeaderAcceptEncoding, "gzip")
	require.NoError(t, sendOkReply(recGz, reqGz, newSuccessHTTPReply("gz")))
	require.EqualValues(t, http.StatusOK, recGz.Code)

	rec2 := httptest.NewRecorder()
	sendErrReply(rec2, req, newErrHTTPReply(proto.ErrParamError))
	require.EqualValues(t, http.StatusOK, rec2.Code)
}

func TestParseSetAndGetConfigParam(t *testing.T) {
	cfg, err := parseSetConfigParam(apiArgsNewGet(t, cfgmetaPartitionInodeIdStep+"=1000"))
	require.NoError(t, err)
	require.Equal(t, "1000", cfg[cfgmetaPartitionInodeIdStep])

	_, err = parseSetConfigParam(apiArgsNewGet(t, ""))
	require.Error(t, err)

	key, err := parseGetConfigParam(apiArgsNewGet(t, "config="+cfgmetaPartitionInodeIdStep))
	require.NoError(t, err)
	require.Equal(t, cfgmetaPartitionInodeIdStep, key)
}

func TestParseQuotaParams(t *testing.T) {
	// GET: ParseForm reads query only; JSON quota paths remain in Body.
	req := httptest.NewRequest(http.MethodGet, "http://x/?name=myvol10&maxFiles=10&maxBytes=100", io.NopCloser(bytes.NewReader([]byte("[]"))))
	sq := &proto.SetMasterQuotaReuqest{}
	err := parseSetQuotaParam(req, sq)
	require.NoError(t, err)

	uq := &proto.UpdateMasterQuotaReuqest{}
	err = parseUpdateQuotaParam(apiArgsNewGet(t, "name=myvol11&quotaId=3&maxFiles=1&maxBytes=2"), uq)
	require.NoError(t, err)
	require.Equal(t, uint32(3), uq.QuotaId)

	vn, qid, err := parseDeleteQuotaParam(apiArgsNewGet(t, "name=myvol12&quotaId=4"))
	require.NoError(t, err)
	require.Equal(t, "myvol12", vn)
	require.Equal(t, uint32(4), qid)

	_, qid, err = parseGetQuotaParam(apiArgsNewGet(t, "name=myvol13&quotaId=5"))
	require.NoError(t, err)
	require.Equal(t, uint32(5), qid)
}

func TestParseRequestToSetTrashInterval(t *testing.T) {
	n, k, iv, err := parseRequestToSetTrashInterval(apiArgsNewGet(t, "name=myvol14&authKey=ak&trashInterval=3600"))
	require.NoError(t, err)
	require.Equal(t, "myvol14", n)
	require.Equal(t, "ak", k)
	require.Equal(t, int64(3600), iv)
}

func TestParseDecommissionAndDiskLimits(t *testing.T) {
	addr, lim, err := parseRequestToUpdateDecommissionFirstHostParallelLimit(apiArgsNewGet(t, "addr=10.0.0.1:17320&decommissionFirstHostParallelLimit=4"))
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:17320", addr)
	require.Equal(t, uint64(4), lim)

	lim2, err := parseRequestToUpdateDecommissionFirstHostDiskParallelLimit(apiArgsNewGet(t, "decommissionFirstHostDiskParallelLimit=5"))
	require.NoError(t, err)
	require.Equal(t, uint64(5), lim2)

	lim3, err := parseRequestToUpdateDecommissionLimit(apiArgsNewGet(t, "decommissionLimit=6"))
	require.NoError(t, err)
	require.Equal(t, uint64(6), lim3)

	lim4, err := parseRequestToUpdateDecommissionDiskLimit(apiArgsNewGet(t, "decommissionDiskLimit=9"))
	require.NoError(t, err)
	require.Equal(t, uint32(9), lim4)
}

func TestParseRequestToResetDpRestoreStatus(t *testing.T) {
	id, err := parseRequestToResetDpRestoreStatus(apiArgsNewGet(t, "id=12345"))
	require.NoError(t, err)
	require.Equal(t, uint64(12345), id)
}

func TestExtractMediaTypeAndStoreMode(t *testing.T) {
	mt, err := extractMediaType(apiArgsNewGet(t, ""))
	require.NoError(t, err)
	require.Equal(t, proto.MediaType_Unspecified, mt)

	mt, err = extractMediaType(apiArgsNewGet(t, "mediaType=2"))
	require.NoError(t, err)
	require.Equal(t, uint32(2), mt)

	vol, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)
	sm, err := parseRocksDbFieldToUpdateVol(apiArgsNewGet(t, "storeMode=1"), vol)
	require.NoError(t, err)
	require.Equal(t, 1, sm)

	sm, err = extractStoreMode(apiArgsNewGet(t, "storeMode=2"))
	require.NoError(t, err)
	require.Equal(t, 2, sm)
}

func TestParseRequestForUpdateNode(t *testing.T) {
	addr, id, tag, err := parseRequestForUpdateNode(apiArgsNewGet(t, "addr=10.0.0.1:17320&id=9&tag=t1"))
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:17320", addr)
	require.Equal(t, uint64(9), id)
	require.Equal(t, "t1", tag)
}

func TestParseRequestToSetApiQpsLimitAndVolCapacity(t *testing.T) {
	n, lim, to, err := parseRequestToSetApiQpsLimit(apiArgsNewGet(t, "name=myvol15&limit=10&timeout=30"))
	require.NoError(t, err)
	require.Equal(t, "myvol15", n)
	require.Equal(t, uint32(10), lim)
	require.Equal(t, uint32(30), to)

	_, _, _, err = parseRequestToSetApiQpsLimit(apiArgsNewGet(t, "name=myvol16&limit=1&timeout=0"))
	require.Error(t, err)

	n2, ak, cap, err := parseRequestToSetVolCapacity(apiArgsNewGet(t, "name=myvol17&authKey=x&capacity=200"))
	require.NoError(t, err)
	require.Equal(t, "myvol17", n2)
	require.Equal(t, "x", ak)
	require.Equal(t, 200, cap)
}

func TestParseRequestToSetDiskBrokenThreshold(t *testing.T) {
	rf, err := parseRequestToSetDiskBrokenThreshold(apiArgsNewGet(t, "markDiskBrokenThreshold=0.85"))
	require.NoError(t, err)
	require.InDelta(t, 0.85, rf, 1e-9)
}

func TestQosArgsIsArgsWork(t *testing.T) {
	require.False(t, (&qosArgs{}).isArgsWork())
	require.True(t, (&qosArgs{iopsRVal: 1}).isArgsWork())
}

func TestValidateRegionName(t *testing.T) {
	require.NoError(t, validateRegionName(""))
	require.NoError(t, validateRegionName("us-west"))
	require.Error(t, validateRegionName("bad name!"))
}

func TestParseStoragePoolRequests(t *testing.T) {
	pi, err := parseRequestToCreateStoragePool(apiArgsNewGet(t, "id=20&name=pooltestx&storageClass=2"))
	require.NoError(t, err)
	require.NotNil(t, pi)
	require.Equal(t, uint8(20), pi.Id)
	require.Equal(t, "pooltestx", pi.Name)

	_, err = parseRequestToCreateStoragePool(apiArgsNewGet(t, "id=0&name=p2&storageClass=2"))
	require.Error(t, err)

	pid, upd, err := parseRequestToUpdateStoragePool(apiArgsNewGet(t, "id=1&name=newpoolname"))
	require.NoError(t, err)
	require.Equal(t, uint8(1), pid)
	require.Equal(t, "newpoolname", upd.Name)

	_, _, err = parseRequestToUpdateStoragePool(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestExtractUintHelpers(t *testing.T) {
	r := apiArgsNewGet(t, "k1=7&k2=8&k3=9&k4=1&k5=2&k6=-1&k7=abc")
	v, err := extractUint(r, "k1")
	require.NoError(t, err)
	require.Equal(t, 7, v)

	_, err = extractUint(r, "k6")
	require.Error(t, err)

	_, err = extractUint64(r, "k7")
	require.Error(t, err)

	u32, err := extractUint32(r, "k2")
	require.NoError(t, err)
	require.Equal(t, uint32(8), u32)

	_, err = extractPositiveUint64(r, "missing")
	require.Error(t, err)

	_, err = extractPositiveUint64(r, "k3")
	require.NoError(t, err)

	v8, err := extractUint8WithDefault(r, "k4", 0)
	require.NoError(t, err)
	require.Equal(t, uint8(1), v8)

	i64, err := extractInt64WithDefault(r, "k5", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), i64)

	require.Equal(t, "def", extractStrWithDefault(apiArgsNewGet(t, ""), "missing", "def"))

	b, err := extractBoolWithDefault(apiArgsNewGet(t, "b=true"), "b", false)
	require.NoError(t, err)
	require.True(t, b)
}

func TestParseS3QosReq(t *testing.T) {
	req := &proto.S3QosRequest{}
	err := parseS3QosReq(apiArgsNewJSONBody(t, map[string]any{"vol": "v"}), req)
	require.NoError(t, err)
}

// TestParseAndExtractSetNodeInfoParams_comprehensive drives most branches in parseAndExtractSetNodeInfoParams (large switch).
func TestParseAndExtractSetNodeInfoParams_comprehensive(t *testing.T) {
	v := url.Values{}
	v.Set(nodeDeleteBatchCountKey, "2")
	v.Set(followerReadLeaseTimeKey, strconv.FormatUint(proto.MinFollowerReadLeaseTimeSec, 10))
	v.Set(nodeMarkDeleteRateKey, "1")
	v.Set(nodeAutoRepairRateKey, "2")
	v.Set(nodeDeleteWorkerSleepMs, "100")
	v.Set(clusterLoadFactorKey, "50.5")
	v.Set(maxDpCntLimitKey, "1000")
	v.Set(maxMpCntLimitKey, "2000")
	v.Set(nodeDpRepairTimeOutKey, "300")
	v.Set(nodeDpBackupKey, "400")
	v.Set(nodeDpMaxRepairErrCntKey, "5")
	v.Set(dpLimitSsdBaseCountKey, "6")
	v.Set(dpLimitSsdFactorKey, "7")
	v.Set(dpLimitHddBaseCountKey, "8")
	v.Set(dpLimitHddFactorKey, "9")
	v.Set(clusterCreateTimeKey, "2020-01-01")
	v.Set(dataNodesetSelectorKey, "dnsel")
	v.Set(metaNodesetSelectorKey, "mnsel")
	v.Set(dataNodeSelectorKey, "dnsel")
	v.Set(metaNodeSelectorKey, "mnsel2")
	v.Set(markDiskBrokenThresholdKey, "0.5")
	v.Set(flashNodeHandleReadTimeout, "1000")
	v.Set(flashHotKeyMissCount, "3")
	v.Set(preheatTotalTask, "4")
	v.Set(maxDisableFlashGroupPercent, "10")
	v.Set(flashNodeReadDataNodeTimeout, "2000")
	v.Set(autoDecommissionDiskKey, "false")
	v.Set(autoDecommissionDiskIntervalKey, "3600")
	v.Set(autoDpMetaRepairKey, "true")
	v.Set(autoDpMetaRepairParallelCntKey, "2")
	v.Set(autoMpMetaRepairKey, "false")
	v.Set(autoMpMetaRepairParallelCntKey, "3")
	v.Set(autoDistributionOptimizationKey, "true")
	v.Set(enableMpDecommissionByLearnerKey, "false")
	v.Set(distributionOptimizationConDpCntKey, "10")
	v.Set(distributionOptimizationThresholdKey, "0.5")
	v.Set(dpTimeoutKey, "60")
	v.Set(mpTimeoutKey, "120")
	v.Set(decommissionLimit, "7")
	v.Set(decommissionDiskLimit, "8")
	v.Set(decommissionFirstHostDiskParallelLimit, "9")
	v.Set(dataMediaTypeKey, "1")
	v.Set(forbidWriteOpOfProtoVersion0, "true")
	v.Set(rackAwareLevelKey, "1")
	v.Set(learnerRecoverTimeoutSecondsKey, "30")
	v.Set(metaAutoAddReplicaLimitKey, "11")
	v.Set(metaManualDecommissionLimitKey, "12")
	v.Set(metaBalanceLimitKey, "13")
	v.Set(metaManualAddReplicaLimitKey, "14")
	v.Set(metaManualLearnerLimitKey, "15")
	v.Set(flashReadFlowLimit, "100")
	v.Set(flashWriteFlowLimit, "200")
	v.Set(flashKeyFlowLimit, "300")
	v.Set(remoteClientFlowLimit, "400")
	v.Set(cfgAutoFixTag, "true")
	v.Set(cfgDefaultDpTag, "dptag")
	v.Set(cfgDefaultMpTag, "mptag")
	v.Set(poolIdKey, "1")
	v.Set(defaultMetaRegionKey, "default")

	m, err := parseAndExtractSetNodeInfoParams(apiArgsNewPostForm(t, v))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(m), 20)
}

func TestParseAndExtractSetNodeInfoParams_invalidUint(t *testing.T) {
	v := url.Values{}
	v.Set(nodeDeleteBatchCountKey, "x")
	_, err := parseAndExtractSetNodeInfoParams(apiArgsNewPostForm(t, v))
	require.Error(t, err)
}

func TestParseSetConfigParam_allListedKeys(t *testing.T) {
	v := url.Values{}
	v.Set(cfgmetaPartitionInodeIdStep, "100")
	v.Set(cfgMetaNodeMemoryHighPer, "0.9")
	v.Set(cfgMetaNodeMemoryLowPer, "0.3")
	v.Set(cfgAutoMpMigrate, "true")
	v.Set(flashNodeHandleReadTimeout, "500")
	v.Set(flashNodeReadDataNodeTimeout, "600")
	v.Set(cfsMpMigrateThreads, "4")
	v.Set(flashHotKeyMissCount, "2")
	v.Set(preheatTotalTask, "3")
	v.Set(maxDisableFlashGroupPercent, "5")
	v.Set(flashReadFlowLimit, "100")
	v.Set(flashWriteFlowLimit, "200")
	v.Set(cfgDefaultVolStoreMode, "1")
	v.Set(cfgDefaultDpTag, "a")
	v.Set(cfgDefaultMpTag, "b")
	cfg, err := parseSetConfigParam(apiArgsNewPostForm(t, v))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(cfg), 10)
}

func TestParseRequestToGetTaskResponse_invalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://x/", bytes.NewReader([]byte("{")))
	_, err := parseRequestToGetTaskResponse(req)
	require.Error(t, err)
}

func TestParseVolVerStrategy_invalidEnable(t *testing.T) {
	_, _, err := parseVolVerStrategy(apiArgsNewGet(t, "enable=notbool"))
	require.Error(t, err)
}

func TestExtractDiskBrokenThreshold_errors(t *testing.T) {
	_, err := extractDiskBrokenThreshold(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestParseRequestToDeleteVol_missingAuth(t *testing.T) {
	_, _, _, _, err := parseRequestToDeleteVol(apiArgsNewGet(t, "name=myvol18"))
	require.Error(t, err)
}

func TestParseRequestToCreateStoragePool_invalidStorageClass(t *testing.T) {
	_, err := parseRequestToCreateStoragePool(apiArgsNewGet(t, "id=21&name=pooly&storageClass=0"))
	require.Error(t, err)
}

func TestParseRequestToUpdateStoragePool_noUpdateFields(t *testing.T) {
	_, _, err := parseRequestToUpdateStoragePool(apiArgsNewGet(t, "id=1"))
	require.Error(t, err)
}

func TestValidateRequestToCreateMetaPartition_countTooHigh(t *testing.T) {
	_, _, _, err := validateRequestToCreateMetaPartition(apiArgsNewGet(t, "count=999&name=myvol19"))
	require.Error(t, err)
}

func TestExtractNodesetID(t *testing.T) {
	r := apiArgsNewGet(t, "id=42")
	require.NoError(t, r.ParseForm())
	id, err := extractNodesetID(r)
	require.NoError(t, err)
	require.Equal(t, uint64(42), id)
}

func TestParseVolUpdateReq_extended(t *testing.T) {
	v0, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)
	vol := copyVolForTest(v0)
	vol.allowedStorageClass = []uint32{vol.volStorageClass}
	vol.allowedPools = []uint8{proto.DefaultSSDPoolId}

	q := url.Values{}
	q.Set(txTimeoutKey, "1")
	q.Set(txConflictRetryNumKey, "100")
	q.Set(txConflictRetryIntervalKey, "500")
	q.Set(authenticateKey, "true")
	q.Set(followerReadKey, "true")
	q.Set(proto.MetaFollowerReadKey, "false")
	q.Set(proto.MetaNearReadKey, "false")
	q.Set(proto.MaximallyReadKey, "false")
	q.Set(proto.VolEnableDirectRead, "false")
	q.Set(proto.VolIgnoreTinyRecover, "false")
	q.Set(dpReadOnlyWhenVolFull, "false")
	q.Set(trashIntervalKey, "100")
	q.Set(accessTimeIntervalKey, strconv.FormatInt(proto.MinAccessTimeValidInterval, 10))
	q.Set(enablePersistAccessTimeKey, "false")
	q.Set(autoDpMetaRepairKey, "false")
	q.Set(autoMpMetaRepairKey, "false")
	q.Set(forbidWriteOpOfProtoVersion0, "true")
	q.Set(dpSelectorNameKey, "seln")
	q.Set(dpSelectorParmKey, "selp")
	q.Set(allowedPoolsKey, fmt.Sprintf("%d", proto.DefaultSSDPoolId))
	q.Set(poolIdKey, fmt.Sprintf("%d", proto.DefaultSSDPoolId))

	u := "http://127.0.0.1/admin?" + q.Encode()
	r := httptest.NewRequest(http.MethodGet, u, nil)
	req := &updateVolReq{}
	err = parseVolUpdateReq(r, &vol, req)
	require.NoError(t, err)
	require.Equal(t, "seln", req.dpSelectorName)
	require.Equal(t, "selp", req.dpSelectorParm)
}

func TestParseRequestToCreateVol_extended(t *testing.T) {
	name := fmt.Sprintf("vx%016x", uint64(time.Now().UnixNano()))
	v := url.Values{}
	v.Set(nameKey, name)
	v.Set(volOwnerKey, "cfs")
	v.Set(replicaNumKey, "3")
	v.Set(volCapacityKey, "100")
	v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
	v.Set(crossZoneKey, "true")
	v.Set(followerReadKey, "true")
	v.Set(StoreModeKey, "1")
	v.Set(remoteCacheEnable, "false")
	v.Set(allowedPoolsKey, fmt.Sprintf("%d,%d", proto.DefaultSSDPoolId, proto.DefaultHDDPoolId))
	v.Set(QosEnableKey, "true")
	v.Set(FlowRKey, "200")
	v.Set(FlowWKey, "200")

	cv := &createVolReq{}
	err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
	require.NoError(t, err)
	require.Equal(t, name, cv.name)
	require.True(t, cv.crossZone)
	require.GreaterOrEqual(t, len(cv.allowedPools), 1)
	require.NotNil(t, cv.qosLimitArgs)
}

func TestParseRequestToCreateVol_invalidStoreMode(t *testing.T) {
	name := fmt.Sprintf("vs%016x", uint64(time.Now().UnixNano()))
	v := url.Values{}
	v.Set(nameKey, name)
	v.Set(volOwnerKey, "cfs")
	v.Set(replicaNumKey, "3")
	v.Set(volCapacityKey, "100")
	v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
	v.Set(StoreModeKey, "99")
	cv := &createVolReq{}
	err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
	require.Error(t, err)
}

func TestParseVolUpdateReq_quotaAndErrors(t *testing.T) {
	v0, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)
	vol := copyVolForTest(v0)
	vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD, proto.StorageClass_Replica_HDD}
	vol.allowedPools = []uint8{proto.DefaultSSDPoolId}
	vol.Capacity = 300

	t.Run("quota_ok", func(t *testing.T) {
		q := url.Values{}
		q.Set(txTimeoutKey, "1")
		q.Set(txConflictRetryNumKey, "100")
		q.Set(txConflictRetryIntervalKey, "500")
		q.Set(volCapacityKey, "300")
		q.Set(quotaClass, "1")
		q.Set(quotaOfClass, "10")
		q.Set(quotaPool, fmt.Sprintf("%d", proto.DefaultSSDPoolId))
		q.Set(quotaOfPool, "5")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err := parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.NoError(t, err)
	})

	t.Run("trash_too_large", func(t *testing.T) {
		q := url.Values{}
		q.Set(txTimeoutKey, "1")
		q.Set(txConflictRetryNumKey, "100")
		q.Set(txConflictRetryIntervalKey, "500")
		q.Set(trashIntervalKey, strconv.FormatInt(int64(maxTrashInterval)+1, 10))
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err := parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("dp_selector_mismatch", func(t *testing.T) {
		q := url.Values{}
		q.Set(txTimeoutKey, "1")
		q.Set(txConflictRetryNumKey, "100")
		q.Set(txConflictRetryIntervalKey, "500")
		q.Set(dpSelectorNameKey, "onlyName")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err := parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})
}

func TestParseVolVerStrategy_enableInvalidCountOrPeriodic(t *testing.T) {
	_, _, err := parseVolVerStrategy(apiArgsNewGet(t, "enable=true&count=notint&periodic=1"))
	require.Error(t, err)

	_, _, err = parseVolVerStrategy(apiArgsNewGet(t, "enable=true&count=2&periodic=notint"))
	require.Error(t, err)
}

func TestParseGetVolParameter_missingAuthWhenNotSkipped(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://x/?name=myvolNoAuth", nil)
	require.NoError(t, err)
	_, err = parseGetVolParameter(req)
	require.Error(t, err)
}

func TestParseVolUpdateReq_branches(t *testing.T) {
	baseTx := func(q url.Values) {
		q.Set(txTimeoutKey, "1")
		q.Set(txConflictRetryNumKey, "100")
		q.Set(txConflictRetryIntervalKey, "500")
	}

	t.Run("invalid_vol_capacity", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD}
		q := url.Values{}
		baseTx(q)
		q.Set(volCapacityKey, "notuint")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("invalid_vol_storage_class", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD, proto.StorageClass_Replica_HDD}
		q := url.Values{}
		baseTx(q)
		q.Set(volStorageClassKey, "notnum")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("quota_class_invalid", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD}
		q := url.Values{}
		baseTx(q)
		q.Set(quotaClass, "3")
		q.Set(quotaOfClass, "1")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("quota_of_class_missing", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD}
		q := url.Values{}
		baseTx(q)
		q.Set(quotaClass, "1")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("quota_of_class_gt_capacity", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD}
		vol.Capacity = 50
		q := url.Values{}
		baseTx(q)
		q.Set(volCapacityKey, "50")
		q.Set(quotaClass, "1")
		q.Set(quotaOfClass, "100")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("quota_pool_not_allowed", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD}
		vol.allowedPools = []uint8{proto.DefaultSSDPoolId}
		q := url.Values{}
		baseTx(q)
		q.Set(quotaPool, "99")
		q.Set(quotaOfPool, "1")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("quota_pool_missing_quota_of_pool", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD}
		vol.allowedPools = []uint8{proto.DefaultSSDPoolId}
		q := url.Values{}
		baseTx(q)
		q.Set(quotaPool, fmt.Sprintf("%d", proto.DefaultSSDPoolId))
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("quota_of_pool_gt_capacity", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD}
		vol.allowedPools = []uint8{proto.DefaultSSDPoolId}
		vol.Capacity = 10
		q := url.Values{}
		baseTx(q)
		q.Set(volCapacityKey, "10")
		q.Set(quotaPool, fmt.Sprintf("%d", proto.DefaultSSDPoolId))
		q.Set(quotaOfPool, "999")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("blob_vol_forbid_class_change", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.volStorageClass = proto.StorageClass_BlobStore
		vol.allowedStorageClass = []uint32{proto.StorageClass_BlobStore}
		q := url.Values{}
		baseTx(q)
		q.Set(volStorageClassKey, fmt.Sprintf("%d", proto.StorageClass_Replica_HDD))
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("replica_class_change_ssd_to_hdd", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.volStorageClass = proto.StorageClass_Replica_SSD
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD, proto.StorageClass_Replica_HDD}
		q := url.Values{}
		baseTx(q)
		q.Set(volStorageClassKey, fmt.Sprintf("%d", proto.StorageClass_Replica_HDD))
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.NoError(t, err)
		require.Equal(t, proto.StorageClass_Replica_HDD, req.volStorageClass)
	})

	t.Run("replica_target_not_in_allowed", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.volStorageClass = proto.StorageClass_Replica_SSD
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD}
		q := url.Values{}
		baseTx(q)
		q.Set(volStorageClassKey, fmt.Sprintf("%d", proto.StorageClass_Replica_HDD))
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("replica_to_blob_denied", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.volStorageClass = proto.StorageClass_Replica_SSD
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD, proto.StorageClass_BlobStore}
		q := url.Values{}
		baseTx(q)
		q.Set(volStorageClassKey, fmt.Sprintf("%d", proto.StorageClass_BlobStore))
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("invalid_default_pool_id", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.allowedStorageClass = []uint32{vol.volStorageClass}
		q := url.Values{}
		baseTx(q)
		q.Set(poolIdKey, "notuint8")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("cold_args_invalid_ebs_on_blob", func(t *testing.T) {
		v0, err := server.cluster.getVol(commonVolName)
		require.NoError(t, err)
		vol := copyVolForTest(v0)
		vol.volStorageClass = proto.StorageClass_BlobStore
		vol.allowedStorageClass = []uint32{proto.StorageClass_BlobStore}
		q := url.Values{}
		baseTx(q)
		q.Set(ebsBlkSizeKey, "bad")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})
}

func TestParseRequestToCreateVol_moreErrors(t *testing.T) {
	t.Run("flow_below_min", func(t *testing.T) {
		name := fmt.Sprintf("vf%016x", uint64(time.Now().UnixNano()))
		v := url.Values{}
		v.Set(nameKey, name)
		v.Set(volOwnerKey, "cfs")
		v.Set(replicaNumKey, "3")
		v.Set(volCapacityKey, "100")
		v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
		v.Set(QosEnableKey, "true")
		v.Set(FlowRKey, "1")
		cv := &createVolReq{}
		err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
		require.Error(t, err)
	})

	t.Run("invalid_default_region", func(t *testing.T) {
		name := fmt.Sprintf("vr%016x", uint64(time.Now().UnixNano()))
		v := url.Values{}
		v.Set(nameKey, name)
		v.Set(volOwnerKey, "cfs")
		v.Set(replicaNumKey, "3")
		v.Set(volCapacityKey, "100")
		v.Set(defaultRegionKey, "__nonexistent_meta_region__")
		cv := &createVolReq{}
		err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
		require.Error(t, err)
	})

	t.Run("follower_read_required_two_replica", func(t *testing.T) {
		name := fmt.Sprintf("v2%016x", uint64(time.Now().UnixNano()))
		v := url.Values{}
		v.Set(nameKey, name)
		v.Set(volOwnerKey, "cfs")
		v.Set(replicaNumKey, "2")
		v.Set(volCapacityKey, "100")
		v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
		v.Set(followerReadKey, "false")
		cv := &createVolReq{}
		err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
		require.Error(t, err)
	})

	t.Run("invalid_allowed_pools_token", func(t *testing.T) {
		name := fmt.Sprintf("vp%016x", uint64(time.Now().UnixNano()))
		v := url.Values{}
		v.Set(nameKey, name)
		v.Set(volOwnerKey, "cfs")
		v.Set(replicaNumKey, "3")
		v.Set(volCapacityKey, "100")
		v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
		v.Set(allowedPoolsKey, "1,x")
		cv := &createVolReq{}
		err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
		require.Error(t, err)
	})

	t.Run("trash_interval_too_large", func(t *testing.T) {
		name := fmt.Sprintf("vti%016x", uint64(time.Now().UnixNano()))
		v := url.Values{}
		v.Set(nameKey, name)
		v.Set(volOwnerKey, "cfs")
		v.Set(replicaNumKey, "3")
		v.Set(volCapacityKey, "100")
		v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
		v.Set(trashIntervalKey, strconv.FormatInt(int64(maxTrashInterval)+1, 10))
		cv := &createVolReq{}
		err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
		require.Error(t, err)
	})

	t.Run("remote_cache_ttl_invalid", func(t *testing.T) {
		name := fmt.Sprintf("vrc%016x", uint64(time.Now().UnixNano()))
		v := url.Values{}
		v.Set(nameKey, name)
		v.Set(volOwnerKey, "cfs")
		v.Set(replicaNumKey, "3")
		v.Set(volCapacityKey, "100")
		v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
		v.Set(remoteCacheTTL, "notint")
		cv := &createVolReq{}
		err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
		require.Error(t, err)
	})

	t.Run("store_mode_non_numeric", func(t *testing.T) {
		name := fmt.Sprintf("vsm%016x", uint64(time.Now().UnixNano()))
		v := url.Values{}
		v.Set(nameKey, name)
		v.Set(volOwnerKey, "cfs")
		v.Set(replicaNumKey, "3")
		v.Set(volCapacityKey, "100")
		v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
		v.Set(StoreModeKey, "x")
		cv := &createVolReq{}
		err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
		require.Error(t, err)
	})

	t.Run("store_mode_invalid_value", func(t *testing.T) {
		name := fmt.Sprintf("vsm2%016x", uint64(time.Now().UnixNano()))
		v := url.Values{}
		v.Set(nameKey, name)
		v.Set(volOwnerKey, "cfs")
		v.Set(replicaNumKey, "3")
		v.Set(volCapacityKey, "100")
		v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
		v.Set(StoreModeKey, "9")
		cv := &createVolReq{}
		err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
		require.Error(t, err)
	})

	t.Run("remote_cache_disable_ttl_invalid_bool", func(t *testing.T) {
		name := fmt.Sprintf("vdt%016x", uint64(time.Now().UnixNano()))
		v := url.Values{}
		v.Set(nameKey, name)
		v.Set(volOwnerKey, "cfs")
		v.Set(replicaNumKey, "3")
		v.Set(volCapacityKey, "100")
		v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
		v.Set(remoteCacheDisableTTL, "maybe")
		cv := &createVolReq{}
		err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
		require.Error(t, err)
	})
}

func TestParseRequestToSetTrashInterval_tooLarge(t *testing.T) {
	q := url.Values{}
	q.Set(nameKey, commonVolName)
	q.Set(volAuthKey, "k")
	q.Set(trashIntervalKey, strconv.FormatInt(int64(maxTrashInterval)+1, 10))
	r := httptest.NewRequest(http.MethodPost, "http://x/", strings.NewReader(q.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	_, _, _, err := parseRequestToSetTrashInterval(r)
	require.Error(t, err)
}

func TestParseRequestToUpdateDecommissionDiskLimit_invalid(t *testing.T) {
	_, err := parseRequestToUpdateDecommissionDiskLimit(apiArgsNewGet(t, fmt.Sprintf("%s=abc", decommissionDiskLimit)))
	require.Error(t, err)
}

func TestParseRequestToUpdateDecommissionDiskLimit_missing(t *testing.T) {
	_, err := parseRequestToUpdateDecommissionDiskLimit(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestExtractUint32WithDefault_invalid(t *testing.T) {
	_, err := extractUint32WithDefault(apiArgsNewGet(t, "volStorageClass=xyz"), volStorageClassKey, 1)
	require.Error(t, err)
}

func TestExtractUint64WithDefault_invalid(t *testing.T) {
	_, err := extractUint64WithDefault(apiArgsNewGet(t, "capacity=bad"), volCapacityKey, 10)
	require.Error(t, err)
}

func TestExtractClientReqInfo_errors(t *testing.T) {
	t.Run("missing_client_message", func(t *testing.T) {
		r := apiArgsNewGet(t, "")
		_, err := extractClientReqInfo(r)
		require.Error(t, err)
	})

	t.Run("invalid_base64", func(t *testing.T) {
		v := url.Values{}
		v.Set(proto.ClientMessage, "%%%not-base64%%%")
		r := httptest.NewRequest(http.MethodPost, "http://x/", strings.NewReader(v.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		_, err := extractClientReqInfo(r)
		require.Error(t, err)
	})
}

func TestParseAndCheckTicket_invalidPlaintextJSON(t *testing.T) {
	plain := []byte("{not valid json")
	msg := base64.StdEncoding.EncodeToString(plain)
	v := url.Values{}
	v.Set(proto.ClientMessage, msg)
	r := httptest.NewRequest(http.MethodPost, "http://x/", strings.NewReader(v.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	var key [32]byte
	_, _, _, err := parseAndCheckTicket(r, key[:], "vol")
	require.Error(t, err)
}

func TestParseVolUpdateReq_quotaParseErrors(t *testing.T) {
	base := func() url.Values {
		q := url.Values{}
		q.Set(txTimeoutKey, "1")
		q.Set(txConflictRetryNumKey, "100")
		q.Set(txConflictRetryIntervalKey, "500")
		return q
	}
	v0, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)
	vol := copyVolForTest(v0)
	vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_SSD}

	t.Run("invalid_quota_class", func(t *testing.T) {
		q := base()
		q.Set(quotaClass, "nope")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("invalid_quota_of_class", func(t *testing.T) {
		q := base()
		q.Set(quotaClass, "1")
		q.Set(quotaOfClass, "x")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("invalid_quota_pool", func(t *testing.T) {
		q := base()
		q.Set(quotaPool, "bad")
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
		require.Error(t, err)
	})

	t.Run("invalid_quota_of_pool", func(t *testing.T) {
		q := base()
		q.Set(quotaPool, fmt.Sprintf("%d", proto.DefaultSSDPoolId))
		q.Set(quotaOfPool, "z")
		vol2 := copyVolForTest(&vol)
		vol2.allowedPools = []uint8{proto.DefaultSSDPoolId}
		u := "http://127.0.0.1/admin?" + q.Encode()
		req := &updateVolReq{}
		err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol2, req)
		require.Error(t, err)
	})
}

func TestParseRequestToUpdateDecommissionFirstHostParallelLimit_missingField(t *testing.T) {
	_, _, err := parseRequestToUpdateDecommissionFirstHostParallelLimit(apiArgsNewGet(t, "addr=10.0.0.1:17320"))
	require.Error(t, err)
}

func TestParseAndCheckTicket_afterVerify_invalidTicket(t *testing.T) {
	jobj := proto.APIAccessReq{
		Type:      proto.MsgMasterCreateVolReq,
		ClientID:  "c",
		ServiceID: proto.MasterServiceID,
		Verifier:  "v",
		Ticket:    "not-a-real-ticket",
	}
	plain, merr := json.Marshal(&jobj)
	require.NoError(t, merr)
	v := url.Values{}
	v.Set(proto.ClientMessage, base64.StdEncoding.EncodeToString(plain))
	r := httptest.NewRequest(http.MethodPost, "http://x/", strings.NewReader(v.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	var key [32]byte
	_, _, _, err := parseAndCheckTicket(r, key[:], "vol")
	require.Error(t, err)
}

func TestCheckTicket_invalidEncoding(t *testing.T) {
	var key [32]byte
	_, err := checkTicket("%%%bad%%%", key[:], proto.MsgMasterCreateVolReq)
	require.Error(t, err)
}

func TestParseSetConfigParam_noListedKeys(t *testing.T) {
	_, err := parseSetConfigParam(apiArgsNewGet(t, "otherKey=1"))
	require.Error(t, err)
}

func TestParseGetConfigParam_missingKey(t *testing.T) {
	_, err := parseGetConfigParam(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestValidateRequestToCreateMetaPartition_invalidCount(t *testing.T) {
	_, _, _, err := validateRequestToCreateMetaPartition(apiArgsNewGet(t, "count=0&name=myvol"))
	require.Error(t, err)
	_, _, _, err = validateRequestToCreateMetaPartition(apiArgsNewGet(t, "count=bad&name=myvol"))
	require.Error(t, err)
}

func TestExtractDecommissionType_missing(t *testing.T) {
	r := apiArgsNewGet(t, "")
	require.NoError(t, r.ParseForm())
	_, err := extractDecommissionType(r)
	require.Error(t, err)
}

func TestParseAndExtractSetNodeInfoParams_moreParseErrors(t *testing.T) {
	cases := []struct {
		key string
		val string
	}{
		{flashReadFlowLimit, "n"},
		{flashWriteFlowLimit, "n"},
		{flashKeyFlowLimit, "n"},
		{remoteClientFlowLimit, "n"},
		{cfgAutoFixTag, "nope"},
		{poolIdKey, "xyz"},
		{decommissionFirstHostDiskParallelLimit, "x"},
		{dataMediaTypeKey, "x"},
		{forbidWriteOpOfProtoVersion0, "nope"},
		{rackAwareLevelKey, "9"},
		{learnerRecoverTimeoutSecondsKey, "0"},
		{metaAutoAddReplicaLimitKey, "x"},
		{metaManualDecommissionLimitKey, "x"},
		{metaBalanceLimitKey, "x"},
		{metaManualAddReplicaLimitKey, "x"},
		{metaManualLearnerLimitKey, "x"},
		{flashNodeReadDataNodeTimeout, "x"},
		{autoDecommissionDiskKey, "x"},
		{autoDecommissionDiskIntervalKey, "x"},
		{autoDpMetaRepairKey, "x"},
		{autoDpMetaRepairParallelCntKey, "x"},
		{autoMpMetaRepairKey, "x"},
		{autoMpMetaRepairParallelCntKey, "x"},
		{autoDistributionOptimizationKey, "x"},
		{enableMpDecommissionByLearnerKey, "x"},
		{distributionOptimizationConDpCntKey, "x"},
		{distributionOptimizationThresholdKey, "x"},
		{dpTimeoutKey, "x"},
		{mpTimeoutKey, "x"},
		{decommissionLimit, "x"},
		{decommissionDiskLimit, "x"},
		// extra keys in parseAndExtractSetNodeInfoParams
		{nodeDpRepairTimeOutKey, "x"},
		{nodeDpBackupKey, "x"},
		{nodeDpMaxRepairErrCntKey, "x"},
		{dpLimitSsdBaseCountKey, "x"},
		{dpLimitSsdFactorKey, "x"},
		{dpLimitHddBaseCountKey, "x"},
		{dpLimitHddFactorKey, "x"},
		{markDiskBrokenThresholdKey, "x"},
		{flashNodeHandleReadTimeout, "x"},
		{flashHotKeyMissCount, "x"},
		{preheatTotalTask, "x"},
		{maxDisableFlashGroupPercent, "x"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			v := url.Values{}
			v.Set(tc.key, tc.val)
			_, err := parseAndExtractSetNodeInfoParams(apiArgsNewPostForm(t, v))
			require.Error(t, err)
		})
	}
}

func TestParseRequestToUpdateDecommissionLimits_errors(t *testing.T) {
	_, err := parseRequestToUpdateDecommissionLimit(apiArgsNewGet(t, ""))
	require.Error(t, err)
	_, err = parseRequestToUpdateDecommissionLimit(apiArgsNewGet(t, fmt.Sprintf("%s=badint", decommissionLimit)))
	require.Error(t, err)

	_, err = parseRequestToUpdateDecommissionFirstHostDiskParallelLimit(apiArgsNewGet(t, ""))
	require.Error(t, err)

	_, _, err = parseRequestToUpdateDecommissionFirstHostParallelLimit(apiArgsNewGet(t, fmt.Sprintf("%s=10", decommissionFirstHostParallelLimit)))
	require.Error(t, err)
}

func TestParseS3QosReq_invalidJSON(t *testing.T) {
	req := &proto.S3QosRequest{}
	err := parseS3QosReq(httptest.NewRequest(http.MethodPost, "http://x/", bytes.NewReader([]byte("{"))), req)
	require.Error(t, err)
}

// TestSendOkReply_partitionAndNodeTypes covers sendOkReply type switches (*MetaPartition, *MetaNode, *DataNode).
func TestSendOkReply_partitionAndNodeTypes(t *testing.T) {
	vol, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)
	req := apiArgsNewGet(t, "")

	for _, mp := range vol.MetaPartitions {
		rec := httptest.NewRecorder()
		require.NoError(t, sendOkReply(rec, req, newSuccessHTTPReply(mp)))
		require.EqualValues(t, http.StatusOK, rec.Code)
		break
	}

	server.cluster.metaNodes.Range(func(_, v interface{}) bool {
		if mn, ok := v.(*MetaNode); ok {
			rec := httptest.NewRecorder()
			require.NoError(t, sendOkReply(rec, req, newSuccessHTTPReply(mn)))
			require.EqualValues(t, http.StatusOK, rec.Code)
			return false
		}
		return true
	})

	server.cluster.dataNodes.Range(func(_, v interface{}) bool {
		if dn, ok := v.(*DataNode); ok {
			rec := httptest.NewRecorder()
			require.NoError(t, sendOkReply(rec, req, newSuccessHTTPReply(dn)))
			require.EqualValues(t, http.StatusOK, rec.Code)
			return false
		}
		return true
	})
}

func TestParseRequestToCreateVol_badOwner(t *testing.T) {
	name := fmt.Sprintf("vown%016x", uint64(time.Now().UnixNano()))
	v := url.Values{}
	v.Set(nameKey, name)
	v.Set(volOwnerKey, "!!!")
	v.Set(replicaNumKey, "3")
	v.Set(volCapacityKey, "100")
	v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
	cv := &createVolReq{}
	err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
	require.Error(t, err)
}

func TestParseRequestToCreateVol_badMpCount(t *testing.T) {
	name := fmt.Sprintf("vmp%016x", uint64(time.Now().UnixNano()))
	v := url.Values{}
	v.Set(nameKey, name)
	v.Set(volOwnerKey, "cfs")
	v.Set(replicaNumKey, "3")
	v.Set(volCapacityKey, "100")
	v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
	v.Set(metaPartitionCountKey, "notint")
	cv := &createVolReq{}
	err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
	require.Error(t, err)
}

func TestValidateRequestToCreateMetaPartition_missingCount(t *testing.T) {
	_, _, _, err := validateRequestToCreateMetaPartition(apiArgsNewGet(t, "name=myvol"))
	require.Error(t, err)
}

func TestParseRequestToCreateVol_invalidDomainId(t *testing.T) {
	name := fmt.Sprintf("vdi%016x", uint64(time.Now().UnixNano()))
	v := url.Values{}
	v.Set(nameKey, name)
	v.Set(volOwnerKey, "cfs")
	v.Set(replicaNumKey, "3")
	v.Set(volCapacityKey, "100")
	v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
	v.Set(domainIdKey, "notuint")
	cv := &createVolReq{}
	err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
	require.Error(t, err)
}

func TestParseSetQuotaParam_invalidMaxFiles(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://x/?name=myvolQ&maxFiles=bad&maxBytes=1", io.NopCloser(bytes.NewReader([]byte("[]"))))
	sq := &proto.SetMasterQuotaReuqest{}
	err := parseSetQuotaParam(req, sq)
	require.Error(t, err)
}

func TestParseSetQuotaParam_invalidPathJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://x/?name=myvolQ2&maxFiles=1&maxBytes=1", io.NopCloser(bytes.NewReader([]byte("{"))))
	sq := &proto.SetMasterQuotaReuqest{}
	err := parseSetQuotaParam(req, sq)
	require.Error(t, err)
}

func TestParseUpdateQuotaParam_invalidQuotaId(t *testing.T) {
	uq := &proto.UpdateMasterQuotaReuqest{}
	err := parseUpdateQuotaParam(apiArgsNewGet(t, "name=myvolUq&quotaId=nope&maxFiles=1&maxBytes=1"), uq)
	require.Error(t, err)
}

func TestExtractMetaPartitionID_missing(t *testing.T) {
	r := apiArgsNewGet(t, "")
	require.NoError(t, r.ParseForm())
	_, err := extractMetaPartitionID(r)
	require.Error(t, err)
}

func TestExtractMetaPartitionID_invalidUint(t *testing.T) {
	r := apiArgsNewGet(t, "id=notuint")
	require.NoError(t, r.ParseForm())
	_, err := extractMetaPartitionID(r)
	require.Error(t, err)
}

func TestParseDeleteQuotaParam_missingQuotaId(t *testing.T) {
	_, _, err := parseDeleteQuotaParam(apiArgsNewGet(t, "name=myvolDelQ"))
	require.Error(t, err)
}

func TestParseGetQuotaParam_missingQuotaId(t *testing.T) {
	_, _, err := parseGetQuotaParam(apiArgsNewGet(t, "name=myvolGetQ"))
	require.Error(t, err)
}

func TestParseRequestToCreateVol_enableQuotaInvalidBool(t *testing.T) {
	name := fmt.Sprintf("veq%016x", uint64(time.Now().UnixNano()))
	v := url.Values{}
	v.Set(nameKey, name)
	v.Set(volOwnerKey, "cfs")
	v.Set(replicaNumKey, "3")
	v.Set(volCapacityKey, "100")
	v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
	v.Set(enableQuota, "notbool")
	cv := &createVolReq{}
	err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
	require.Error(t, err)
}

func TestParseRequestToCreateVol_allowedPoolsUnknownPool(t *testing.T) {
	name := fmt.Sprintf("vap%016x", uint64(time.Now().UnixNano()))
	v := url.Values{}
	v.Set(nameKey, name)
	v.Set(volOwnerKey, "cfs")
	v.Set(replicaNumKey, "3")
	v.Set(volCapacityKey, "100")
	v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
	v.Set(allowedPoolsKey, "1,254")
	cv := &createVolReq{}
	err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
	require.Error(t, err)
}

func TestParseRequestToCreateVol_remoteCacheEnableInvalidBool(t *testing.T) {
	name := fmt.Sprintf("vrce%016x", uint64(time.Now().UnixNano()))
	v := url.Values{}
	v.Set(nameKey, name)
	v.Set(volOwnerKey, "cfs")
	v.Set(replicaNumKey, "3")
	v.Set(volCapacityKey, "100")
	v.Set(defaultRegionKey, server.cluster.defaultMetaRegion)
	v.Set(remoteCacheEnable, "notbool")
	cv := &createVolReq{}
	err := parseRequestToCreateVol(apiArgsNewPostForm(t, v), cv, server)
	require.Error(t, err)
}

func TestExtractMetaPartitionIDAndAddr_missingAddr(t *testing.T) {
	_, _, err := extractMetaPartitionIDAndAddr(apiArgsNewGet(t, "id=1"))
	require.Error(t, err)
}

func TestExtractDataPartitionIDAndAddr_missingAddr(t *testing.T) {
	_, _, err := extractDataPartitionIDAndAddr(apiArgsNewGet(t, "id=1"))
	require.Error(t, err)
}

func TestParseRequestToUpdateDecommissionFirstHostParallelLimit_badLimit(t *testing.T) {
	_, _, err := parseRequestToUpdateDecommissionFirstHostParallelLimit(
		apiArgsNewGet(t, "addr=10.0.0.1:17320&decommissionFirstHostParallelLimit=notuint"))
	require.Error(t, err)
}

func TestParseVolUpdateReq_invalidEnablePosixAcl(t *testing.T) {
	v0, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)
	vol := copyVolForTest(v0)
	vol.allowedStorageClass = []uint32{vol.volStorageClass}
	q := url.Values{}
	q.Set(txTimeoutKey, "1")
	q.Set(txConflictRetryNumKey, "100")
	q.Set(txConflictRetryIntervalKey, "500")
	q.Set(enablePosixAclKey, "notbool")
	u := "http://127.0.0.1/admin?" + q.Encode()
	req := &updateVolReq{}
	err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
	require.Error(t, err)
}

func TestParseVolUpdateReq_invalidTxForceReset(t *testing.T) {
	v0, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)
	vol := copyVolForTest(v0)
	vol.allowedStorageClass = []uint32{vol.volStorageClass}
	q := url.Values{}
	q.Set(txTimeoutKey, "1")
	q.Set(txConflictRetryNumKey, "100")
	q.Set(txConflictRetryIntervalKey, "500")
	q.Set(enableTxMaskKey, "rename")
	q.Set(txForceResetKey, "bad")
	u := "http://127.0.0.1/admin?" + q.Encode()
	req := &updateVolReq{}
	err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
	require.Error(t, err)
}

func TestParseRequestToSetApiQpsLimit_zeroTimeout(t *testing.T) {
	_, _, _, err := parseRequestToSetApiQpsLimit(apiArgsNewGet(t, "name=myvolQps&limit=10&timeout=0"))
	require.Error(t, err)
}

func TestParseRequestToSetVolCapacity_invalidCapacity(t *testing.T) {
	_, _, _, err := parseRequestToSetVolCapacity(apiArgsNewGet(t, "name=myvolCap&authKey=k&capacity=bad"))
	require.Error(t, err)
}

func TestParseVolUpdateReq_invalidTxMaskString(t *testing.T) {
	v0, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)
	vol := copyVolForTest(v0)
	vol.allowedStorageClass = []uint32{vol.volStorageClass}
	q := url.Values{}
	q.Set(txTimeoutKey, "1")
	q.Set(txConflictRetryNumKey, "100")
	q.Set(txConflictRetryIntervalKey, "500")
	q.Set(enableTxMaskKey, "___not_a_mask___")
	q.Set(txForceResetKey, "false")
	u := "http://127.0.0.1/admin?" + q.Encode()
	req := &updateVolReq{}
	err = parseVolUpdateReq(httptest.NewRequest(http.MethodGet, u, nil), &vol, req)
	require.Error(t, err)
}

func TestExtractClientIDKey_missing(t *testing.T) {
	r := apiArgsNewGet(t, "")
	require.NoError(t, r.ParseForm())
	_, err := extractClientIDKey(r)
	require.Error(t, err)
}

func TestParseAndExtractPartitionInfo_errors(t *testing.T) {
	_, err := parseAndExtractPartitionInfo(apiArgsNewGet(t, ""))
	require.Error(t, err)
	_, err = parseAndExtractPartitionInfo(apiArgsNewGet(t, "id=notuint"))
	require.Error(t, err)
}

// --- additional helpers coverage ---

func TestExtractStoreModeAndRocksDb(t *testing.T) {
	// extractStoreMode: normal
	sm, err := extractStoreMode(apiArgsNewGet(t, "storeMode=1"))
	require.NoError(t, err)
	require.Equal(t, 1, sm)

	// extractStoreMode: empty value → 0, no error
	sm, err = extractStoreMode(apiArgsNewGet(t, ""))
	require.NoError(t, err)
	require.Equal(t, 0, sm)

	// extractStoreMode: invalid value
	_, err = extractStoreMode(apiArgsNewGet(t, "storeMode=bad"))
	require.Error(t, err)

	// parseRocksDbFieldToUpdateVol: uses vol.DefaultStoreMode as default when key absent
	vol, verr := server.cluster.getVol(commonVolName)
	require.NoError(t, verr)
	sm, err = parseRocksDbFieldToUpdateVol(apiArgsNewGet(t, ""), vol)
	require.NoError(t, err)
	require.Equal(t, int(vol.DefaultStoreMode), sm)
}

func TestParseRequestToCreateStoragePool_invalidCId(t *testing.T) {
	// storageClass=1 (valid ReplicaSSD), cId is not an int
	_, err := parseRequestToCreateStoragePool(apiArgsNewGet(t, "id=1&name=poolx&storageClass=1&cId=notint"))
	require.Error(t, err)
}

func TestParseRequestToCreateStoragePool_invalidPoolIdRange(t *testing.T) {
	// id=0 is out of range
	_, err := parseRequestToCreateStoragePool(apiArgsNewGet(t, "id=0&name=poolx&storageClass=1"))
	require.Error(t, err)
}

func TestParseRequestToBalanceMetaPartition_empty(t *testing.T) {
	// succeeds even with empty params (both fields optional)
	zones, nsids, err := parseRequestToBalanceMetaPartition(apiArgsNewGet(t, ""))
	require.NoError(t, err)
	require.Empty(t, zones)
	require.Empty(t, nsids)
}

func TestParseAndExtractThreshold_errors(t *testing.T) {
	_, err := parseAndExtractThreshold(apiArgsNewGet(t, ""))
	require.Error(t, err)
	_, err = parseAndExtractThreshold(apiArgsNewGet(t, "threshold=notfloat"))
	require.Error(t, err)
}

func TestParseAndExtractVolDeletionDelayTime_errors(t *testing.T) {
	_, err := parseAndExtractVolDeletionDelayTime(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestParseAndExtractFlashTopoDeletionDelayTime_errors(t *testing.T) {
	_, err := parseAndExtractFlashTopoDeletionDelayTime(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestParseAndExtractMetaNodeGOGC_errors(t *testing.T) {
	_, err := parseAndExtractMetaNodeGOGC(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestParseAndExtractDataNodeGOGC_errors(t *testing.T) {
	_, err := parseAndExtractDataNodeGOGC(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestParseAndExtractFileStatsThresholds_emptyKey(t *testing.T) {
	_, err := parseAndExtractFileStatsThresholds(apiArgsNewGet(t, ""))
	require.Error(t, err)
}

func TestParseAndExtractSetNodeSetInfoParams_invalidCount(t *testing.T) {
	_, err := parseAndExtractSetNodeSetInfoParams(apiArgsNewGet(t, "count=bad&id=1"))
	require.Error(t, err)
}

func TestParseAndExtractSetNodeSetInfoParams_invalidId(t *testing.T) {
	_, err := parseAndExtractSetNodeSetInfoParams(apiArgsNewGet(t, "count=1&id=notint"))
	require.Error(t, err)
}

func TestExtractPositiveUint64_errors(t *testing.T) {
	r := apiArgsNewGet(t, "")
	require.NoError(t, r.ParseForm())
	// missing key
	_, err := extractPositiveUint64(r, "myKey")
	require.Error(t, err)
	// zero value
	r2 := apiArgsNewGet(t, "myKey=0")
	require.NoError(t, r2.ParseForm())
	_, err = extractPositiveUint64(r2, "myKey")
	require.Error(t, err)
}

func TestExtractMediaType_invalid(t *testing.T) {
	r := apiArgsNewGet(t, "mediaType=notint")
	require.NoError(t, r.ParseForm())
	_, err := extractMediaType(r)
	require.Error(t, err)
}

func TestParseRequestToUpdateStoragePool_invalidPoolId(t *testing.T) {
	_, _, err := parseRequestToUpdateStoragePool(apiArgsNewGet(t, "id=notint&name=p1"))
	require.Error(t, err)
}
