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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------------------
// helpers
// -------------------------------------------------------------------------

// userPost sends an HTTP POST to the running master server and returns the
// decoded reply without asserting on the reply code.
func userPost(t *testing.T, path string, body interface{}) *proto.HTTPReply {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)

	resp, err := http.Post(hostAddr+path, "application/json", bytes.NewReader(data))
	require.NoError(t, err)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	reply := &proto.HTTPReply{}
	require.NoError(t, json.Unmarshal(raw, reply))
	return reply
}

// userGet sends an HTTP GET to the running master server without asserting
// on the reply code.
func userGet(t *testing.T, path string) *proto.HTTPReply {
	t.Helper()
	resp, err := http.Get(hostAddr + path)
	require.NoError(t, err)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	reply := &proto.HTTPReply{}
	require.NoError(t, json.Unmarshal(raw, reply))
	return reply
}

// errReader is a body that always returns a read error, used to exercise the
// io.ReadAll failure branch in handlers that read the request body.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, fmt.Errorf("injected read error") }

// callHandler invokes an http.HandlerFunc through httptest machinery.
func callHandler(fn http.HandlerFunc, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	fn(w, r)
	return w
}

// decodeReply unmarshals the httptest response body into an HTTPReply.
func decodeReply(t *testing.T, w *httptest.ResponseRecorder) *proto.HTTPReply {
	t.Helper()
	reply := &proto.HTTPReply{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), reply))
	return reply
}

// -------------------------------------------------------------------------
// parseUser / extractUser
// -------------------------------------------------------------------------

func TestParseUser_Valid(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/?user=alice", nil)
	uid, err := parseUser(r)
	require.NoError(t, err)
	require.Equal(t, "alice", uid)
}

func TestParseUser_MissingKey(t *testing.T) {
	// No "user" query param → extractUser returns keyNotFound.
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	_, err := parseUser(r)
	require.Error(t, err)
}

func TestExtractUser_MissingKey(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, r.ParseForm())
	_, err := extractUser(r)
	require.Error(t, err)
}

func TestExtractUser_Valid(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/?user=bob", nil)
	require.NoError(t, r.ParseForm())
	u, err := extractUser(r)
	require.NoError(t, err)
	require.Equal(t, "bob", u)
}

// -------------------------------------------------------------------------
// parseAccessKey / extractAccessKey
// -------------------------------------------------------------------------

func TestParseAccessKey_Missing(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	_, err := parseAccessKey(r)
	require.Error(t, err)
}

func TestParseAccessKey_InvalidFormat(t *testing.T) {
	// AKRegexp requires exactly 16 alphanumeric characters.
	r, _ := http.NewRequest(http.MethodGet, "/?ak=short", nil)
	_, err := parseAccessKey(r)
	require.Error(t, err)
}

func TestParseAccessKey_Valid(t *testing.T) {
	// Exactly 16 alphanumeric characters.
	r, _ := http.NewRequest(http.MethodGet, "/?ak=0123456789abcdef", nil)
	gotAK, err := parseAccessKey(r)
	require.NoError(t, err)
	require.Equal(t, "0123456789abcdef", gotAK)
}

func TestExtractAccessKey_Missing(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, r.ParseForm())
	_, err := extractAccessKey(r)
	require.Error(t, err)
}

func TestExtractAccessKey_InvalidFormat(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/?ak=BAD!", nil)
	require.NoError(t, r.ParseForm())
	_, err := extractAccessKey(r)
	require.Error(t, err)
}

func TestExtractAccessKey_Valid(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/?ak=AAAAAAAAAAAAAAAA", nil)
	require.NoError(t, r.ParseForm())
	ak, err := extractAccessKey(r)
	require.NoError(t, err)
	require.Equal(t, "AAAAAAAAAAAAAAAA", ak)
}

// -------------------------------------------------------------------------
// parseKeywords / extractKeywords
// -------------------------------------------------------------------------

func TestParseKeywords_Empty(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	kw, err := parseKeywords(r)
	require.NoError(t, err)
	require.Empty(t, kw)
}

func TestParseKeywords_WithValue(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/?keywords=test", nil)
	kw, err := parseKeywords(r)
	require.NoError(t, err)
	require.Equal(t, "test", kw)
}

func TestExtractKeywords(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/?keywords=mykey", nil)
	require.NoError(t, r.ParseForm())
	kw := extractKeywords(r)
	require.Equal(t, "mykey", kw)
}

// -------------------------------------------------------------------------
// createUser handler — error paths (direct handler invocation)
// -------------------------------------------------------------------------

func TestCreateUser_ReadBodyError(t *testing.T) {
	// io.ReadAll(r.Body) fails → ErrCodeParamError reply.
	r, _ := http.NewRequest(http.MethodPost, proto.UserCreate, errReader{})
	w := callHandler(server.createUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestCreateUser_BadJSON(t *testing.T) {
	// json.Unmarshal fails → ErrCodeParamError reply.
	r, _ := http.NewRequest(http.MethodPost, proto.UserCreate, strings.NewReader("not-json"))
	w := callHandler(server.createUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestCreateUser_InvalidUserID(t *testing.T) {
	// ownerRegexp fails for IDs with special characters.
	param := proto.UserCreateParam{ID: "!!!invalid", Type: proto.UserTypeNormal}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserCreate, bytes.NewReader(data))
	w := callHandler(server.createUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestCreateUser_RootUserType(t *testing.T) {
	// UserTypeRoot is forbidden → ErrInvalidUserType.
	param := proto.UserCreateParam{ID: "validuser", Type: proto.UserTypeRoot}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserCreate, bytes.NewReader(data))
	w := callHandler(server.createUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestCreateUser_DuplicateUser(t *testing.T) {
	// Creating the same user ID twice → createKey returns an error.
	const dupUID = "dup_user_test"
	paramFirst := proto.UserCreateParam{ID: dupUID, Type: proto.UserTypeNormal}
	dataFirst, _ := json.Marshal(paramFirst)

	// First creation succeeds (ignore error if already exists from prior run).
	r1, _ := http.NewRequest(http.MethodPost, proto.UserCreate, bytes.NewReader(dataFirst))
	callHandler(server.createUser, r1)

	// Second creation must fail.
	dataSecond, _ := json.Marshal(paramFirst)
	r2, _ := http.NewRequest(http.MethodPost, proto.UserCreate, bytes.NewReader(dataSecond))
	w2 := callHandler(server.createUser, r2)
	reply := decodeReply(t, w2)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)

	// Clean up.
	server.user.deleteKey(dupUID) //nolint:errcheck
}

// -------------------------------------------------------------------------
// deleteUser handler — error paths
// -------------------------------------------------------------------------

func TestDeleteUser_MissingParam(t *testing.T) {
	// No "user" query param → parseUser returns error.
	r, _ := http.NewRequest(http.MethodGet, proto.UserDelete, nil)
	w := callHandler(server.deleteUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestDeleteUser_NotFound(t *testing.T) {
	// Non-existent user → deleteKey returns error.
	r, _ := http.NewRequest(http.MethodGet, proto.UserDelete+"?user=__nonexistent_user__", nil)
	w := callHandler(server.deleteUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// updateUser handler — error paths
// -------------------------------------------------------------------------

func TestUpdateUser_ReadBodyError(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, proto.UserUpdate, errReader{})
	w := callHandler(server.updateUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestUpdateUser_BadJSON(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, proto.UserUpdate, strings.NewReader("{bad"))
	w := callHandler(server.updateUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestUpdateUser_RootUserType(t *testing.T) {
	param := proto.UserUpdateParam{UserID: "cfs", Type: proto.UserTypeRoot}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserUpdate, bytes.NewReader(data))
	w := callHandler(server.updateUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestUpdateUser_NotFound(t *testing.T) {
	// Updating a user that doesn't exist → updateKey returns error.
	param := proto.UserUpdateParam{UserID: "__no_user__", Type: proto.UserTypeNormal}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserUpdate, bytes.NewReader(data))
	w := callHandler(server.updateUser, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// getUserAKInfo handler — error paths
// -------------------------------------------------------------------------

func TestGetUserAKInfo_MissingAK(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, proto.UserGetAKInfo, nil)
	w := callHandler(server.getUserAKInfo, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestGetUserAKInfo_InvalidAKFormat(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, proto.UserGetAKInfo+"?ak=bad!", nil)
	w := callHandler(server.getUserAKInfo, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestGetUserAKInfo_AKNotFound(t *testing.T) {
	// 16 alphanumeric chars but not registered.
	r, _ := http.NewRequest(http.MethodGet, proto.UserGetAKInfo+"?ak=ZZZZZZZZZZZZZZZZ", nil)
	w := callHandler(server.getUserAKInfo, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// getUserInfo handler — error paths
// -------------------------------------------------------------------------

func TestGetUserInfo_MissingParam(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, proto.UserGetInfo, nil)
	w := callHandler(server.getUserInfo, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestGetUserInfo_NotFound(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, proto.UserGetInfo+"?user=__nonexistent__", nil)
	w := callHandler(server.getUserInfo, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// updateUserPolicy handler — error paths
// -------------------------------------------------------------------------

func TestUpdateUserPolicy_ReadBodyError(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, proto.UserUpdatePolicy, errReader{})
	w := callHandler(server.updateUserPolicy, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestUpdateUserPolicy_BadJSON(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, proto.UserUpdatePolicy, strings.NewReader("{bad"))
	w := callHandler(server.updateUserPolicy, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestUpdateUserPolicy_VolNotExists(t *testing.T) {
	param := proto.UserPermUpdateParam{UserID: "cfs", Volume: "__no_vol__"}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserUpdatePolicy, bytes.NewReader(data))
	w := callHandler(server.updateUserPolicy, r)
	reply := decodeReply(t, w)
	require.Equal(t, int32(proto.ErrCodeVolNotExists), reply.Code)
}

func TestUpdateUserPolicy_UserNotExists(t *testing.T) {
	// Vol exists, but user doesn't → updatePolicy returns error.
	param := proto.UserPermUpdateParam{UserID: "__no_user__", Volume: commonVolName, Policy: []string{proto.BuiltinPermissionWritable.String()}}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserUpdatePolicy, bytes.NewReader(data))
	w := callHandler(server.updateUserPolicy, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// removeUserPolicy handler — error paths
// -------------------------------------------------------------------------

func TestRemoveUserPolicy_ReadBodyError(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, proto.UserRemovePolicy, errReader{})
	w := callHandler(server.removeUserPolicy, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestRemoveUserPolicy_BadJSON(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, proto.UserRemovePolicy, strings.NewReader("[invalid"))
	w := callHandler(server.removeUserPolicy, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestRemoveUserPolicy_VolNotExists(t *testing.T) {
	param := proto.UserPermRemoveParam{UserID: "cfs", Volume: "__no_vol__"}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserRemovePolicy, bytes.NewReader(data))
	w := callHandler(server.removeUserPolicy, r)
	reply := decodeReply(t, w)
	require.Equal(t, int32(proto.ErrCodeVolNotExists), reply.Code)
}

func TestRemoveUserPolicy_UserNotExists(t *testing.T) {
	// Vol exists, user doesn't → removePolicy returns error.
	param := proto.UserPermRemoveParam{UserID: "__no_user__", Volume: commonVolName}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserRemovePolicy, bytes.NewReader(data))
	w := callHandler(server.removeUserPolicy, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// deleteUserVolPolicy handler — error paths
// -------------------------------------------------------------------------

func TestDeleteUserVolPolicy_MissingName(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, proto.UserDeleteVolPolicy, nil)
	w := callHandler(server.deleteUserVolPolicy, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestDeleteUserVolPolicy_Success(t *testing.T) {
	// A vol that has no policy can still be "cleared" without error.
	r, _ := http.NewRequest(http.MethodGet, proto.UserDeleteVolPolicy+"?name="+commonVolName, nil)
	w := callHandler(server.deleteUserVolPolicy, r)
	reply := decodeReply(t, w)
	require.Equal(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// transferUserVol handler — error paths
// -------------------------------------------------------------------------

func TestTransferUserVol_ReadBodyError(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, proto.UserTransferVol, errReader{})
	w := callHandler(server.transferUserVol, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestTransferUserVol_BadJSON(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, proto.UserTransferVol, strings.NewReader("{{invalid"))
	w := callHandler(server.transferUserVol, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestTransferUserVol_VolNotExists(t *testing.T) {
	param := proto.UserTransferVolParam{Volume: "__no_vol__", UserSrc: "cfs", UserDst: "cfs", Force: true}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserTransferVol, bytes.NewReader(data))
	w := callHandler(server.transferUserVol, r)
	reply := decodeReply(t, w)
	require.Equal(t, int32(proto.ErrCodeVolNotExists), reply.Code)
}

func TestTransferUserVol_OwnerMismatchNoForce(t *testing.T) {
	// Vol exists, Force=false, UserSrc does not match vol.Owner → ErrHaveNoPolicy.
	vol, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)

	param := proto.UserTransferVolParam{
		Volume:  commonVolName,
		UserSrc: "__wrong_owner__", // guaranteed mismatch
		UserDst: "cfs",
		Force:   false,
	}
	// Make sure the actual owner is different from UserSrc.
	require.NotEqual(t, param.UserSrc, vol.Owner)

	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserTransferVol, bytes.NewReader(data))
	w := callHandler(server.transferUserVol, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestTransferUserVol_UserDstNotExists(t *testing.T) {
	// Force=true bypasses owner check; transferVol fails if dst user doesn't exist.
	vol, err := server.cluster.getVol(commonVolName)
	require.NoError(t, err)

	param := proto.UserTransferVolParam{
		Volume:  commonVolName,
		UserSrc: vol.Owner,
		UserDst: "__no_dst_user__",
		Force:   true,
	}
	data, _ := json.Marshal(param)
	r, _ := http.NewRequest(http.MethodPost, proto.UserTransferVol, bytes.NewReader(data))
	w := callHandler(server.transferUserVol, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// getAllUsers handler — via HTTP server (keywords path)
// -------------------------------------------------------------------------

func TestGetAllUsers_WithKeywords(t *testing.T) {
	// Exercises the normal path and the "keywords" param extraction.
	reply := userGet(t, fmt.Sprintf("%v?keywords=cfs", proto.UserList))
	require.Equal(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestGetAllUsers_EmptyKeywords(t *testing.T) {
	reply := userGet(t, proto.UserList)
	require.Equal(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// getUsersOfVol handler — fully uncovered before this test suite
// -------------------------------------------------------------------------

func TestGetUsersOfVol_MissingName(t *testing.T) {
	// No "name" query param → parseVolName returns keyNotFound error.
	r, _ := http.NewRequest(http.MethodGet, proto.UsersOfVol, nil)
	w := callHandler(server.getUsersOfVol, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestGetUsersOfVol_VolNotExists(t *testing.T) {
	// Existing handler exercises getUsersOfVol error path.
	r, _ := http.NewRequest(http.MethodGet, proto.UsersOfVol+"?name=__no_vol__", nil)
	w := callHandler(server.getUsersOfVol, r)
	reply := decodeReply(t, w)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestGetUsersOfVol_Success(t *testing.T) {
	// Ensure commonVol has at least one user policy entry so that
	// getUsersOfVol returns success regardless of other tests' ordering.
	setupParam := &proto.UserPermUpdateParam{
		UserID: "cfs",
		Volume: commonVolName,
		Policy: []string{proto.BuiltinPermissionWritable.String()},
	}
	_, err := server.user.updatePolicy(setupParam)
	require.NoError(t, err)

	r, _ := http.NewRequest(http.MethodGet, proto.UsersOfVol+"?name="+commonVolName, nil)
	w := callHandler(server.getUsersOfVol, r)
	reply := decodeReply(t, w)
	require.Equal(t, int32(proto.ErrCodeSuccess), reply.Code)
}

// -------------------------------------------------------------------------
// End-to-end smoke tests that exercise the full HTTP path (router → handler).
// These complement the direct handler tests above.
// -------------------------------------------------------------------------

func TestAPIServiceUser_CreateAndDeleteUser(t *testing.T) {
	const uid = "api_test_user_smoke"

	// Create
	reply := userPost(t, proto.UserCreate, proto.UserCreateParam{ID: uid, Type: proto.UserTypeNormal})
	require.Equal(t, int32(proto.ErrCodeSuccess), reply.Code)

	// Get
	reply = userGet(t, fmt.Sprintf("%v?user=%v", proto.UserGetInfo, uid))
	require.Equal(t, int32(proto.ErrCodeSuccess), reply.Code)

	// Delete
	reply = userGet(t, fmt.Sprintf("%v?user=%v", proto.UserDelete, uid))
	require.Equal(t, int32(proto.ErrCodeSuccess), reply.Code)

	// Confirm gone
	_, err := server.user.getUserInfo(uid)
	require.Error(t, err)
}

func TestAPIServiceUser_GetAKInfo_InvalidAndMissing(t *testing.T) {
	// Missing AK
	reply := userGet(t, proto.UserGetAKInfo)
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)

	// Invalid AK format (too short)
	reply = userGet(t, proto.UserGetAKInfo+"?ak=short")
	require.NotEqual(t, int32(proto.ErrCodeSuccess), reply.Code)
}

func TestAPIServiceUser_PolicyErrors(t *testing.T) {
	// updateUserPolicy: vol not found
	reply := userPost(t, proto.UserUpdatePolicy, proto.UserPermUpdateParam{
		UserID: "cfs", Volume: "__ghost_vol__",
	})
	require.Equal(t, int32(proto.ErrCodeVolNotExists), reply.Code)

	// removeUserPolicy: vol not found
	reply = userPost(t, proto.UserRemovePolicy, proto.UserPermRemoveParam{
		UserID: "cfs", Volume: "__ghost_vol__",
	})
	require.Equal(t, int32(proto.ErrCodeVolNotExists), reply.Code)
}
