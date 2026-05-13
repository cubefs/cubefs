package cmd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cubefs/cubefs/sdk/master"
	"github.com/stretchr/testify/require"
)

func TestVolCreateCmd_MinReadAheadSize(t *testing.T) {
	// Mock errout to catch the error instead of calling os.Exit
	var caughtErr error
	originalErrout := errout
	errout = func(err error) {
		caughtErr = err
	}
	defer func() {
		errout = originalErrout
	}()

	// Mock server to return a valid response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"code":0,"msg":"success","data":{}}`))
	}))
	defer server.Close()

	client := master.NewMasterClient([]string{server.URL[7:]}, false) // remove "http://"
	cmd := newVolCreateCmd(client)
	cmd.ParseFlags([]string{"--minReadAheadSize", "1"})
	// Need to provide arguments for the command
	cmd.Run(cmd, []string{"test_vol", "test_user"})
	require.NotNil(t, caughtErr)
	require.Contains(t, caughtErr.Error(), "minReadAheadSize")
}

func TestVolUpdateCmd_MinReadAheadSize(t *testing.T) {
	// Mock errout to catch the error instead of calling os.Exit
	var caughtErr error
	originalErrout := errout
	errout = func(err error) {
		caughtErr = err
	}
	defer func() {
		errout = originalErrout
	}()

	// Mock server to return a valid SimpleVolView for GetVolumeSimpleInfo
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return a valid JSON matching proto.HTTPReplyRaw wrapping proto.SimpleVolView
		w.Write([]byte(`{"code":0,"msg":"success","data":{"Name":"test_vol"}}`))
	}))
	defer server.Close()

	client := master.NewMasterClient([]string{server.URL[7:]}, false) // remove "http://"
	cmd := newVolUpdateCmd(client)
	cmd.ParseFlags([]string{"--minReadAheadSize", "1"})
	cmd.Run(cmd, []string{"test_vol"})
	require.NotNil(t, caughtErr)
	require.Contains(t, caughtErr.Error(), "minReadAheadSize")
}
