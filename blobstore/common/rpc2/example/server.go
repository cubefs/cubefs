package main

import (
	"bytes"
	"io"
	"os"
	"path"

	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type Config struct {
	cmd.Config
}

func init() {
	mod := &cmd.Module{
		Name:       "example_rpc2",
		InitConfig: initConfig,
		SetUp2:     setUp2,
		TearDown:   func() {},
	}
	cmd.RegisterGracefulModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	var conf Config
	config.Init("f", "", "server.conf")
	if err := config.Load(&conf); err != nil {
		return nil, err
	}
	logDir := path.Join(os.TempDir(), "example_rpc2")
	os.MkdirAll(logDir, 0o644)
	conf.AuditLog.LogDir = logDir
	conf.LogConf.Filename = path.Join(logDir, "rpc2.log")
	conf.Rpc2Server.Addresses = []rpc2.NetworkAddress{
		{Network: "tcp", Address: listenon[0]},
		{Network: "tcp", Address: listenon[1]},
	}
	return &conf.Config, nil
}

func setUp2() (*rpc2.Router, []rpc2.Interceptor) {
	router := &rpc2.Router{}
	router.Middleware(handleMiddleware1, handleMiddleware2)
	router.Register("/ping", handlePing)
	router.Register("/kick", handleKick)
	router.Register("/stream", handleStream)
	return router, nil
}

func handleMiddleware1(w rpc2.ResponseWriter, req *rpc2.Request) error {
	log.Info("middleware-1")
	return nil
}

func handleMiddleware2(w rpc2.ResponseWriter, req *rpc2.Request) error {
	log.Info("middleware-2")
	return nil
}

func handlePing(w rpc2.ResponseWriter, req *rpc2.Request) error {
	log.Info(req.RequestHeader.String())
	var para pingPara
	req.ParseParameter(&para)

	resp := bytes.NewBuffer(nil)
	resp.WriteString("response -> ")
	w.SetContentLength(int64(resp.Len()) + req.ContentLength)
	buff := make([]byte, req.ContentLength)
	if _, err := io.ReadFull(req.Body, buff); err != nil {
		return err
	}
	req.Body.Close()
	log.Info("body   :", string(buff))
	log.Info("trailer:", req.Trailer.M)
	w.Trailer().SetLen("server-trailer", 3)
	w.AfterBody(func() error {
		log.Info("run after body stack - 1")
		w.Trailer().Set("server-trailer", "123")
		return nil
	})
	w.AfterBody(func() error {
		log.Info("run after body stack - 2")
		return nil
	})

	w.WriteHeader(200, &para)
	w.Header().Set("ignored", "x") // ignore
	resp.Write(buff)
	_, err := w.ReadFrom(resp)
	return err
}

func handleKick(w rpc2.ResponseWriter, req *rpc2.Request) error {
	var para pingPara
	req.ParseParameter(&para)
	para.S = "response -> " + para.S
	return w.WriteOK(&para)
}

func handleStream(_ rpc2.ResponseWriter, req *rpc2.Request) error {
	var para pingPara
	req.ParseParameter(&para)
	para.S = "response -> " + para.S

	stream := rpc2.GenericServerStream[streamReq, streamResp]{ServerStream: req.ServerStream()}
	var header, trailer rpc2.Header
	header.Set("stream-header-a", "aaa")
	trailer.Set("stream-trailer-b", "")
	stream.SetHeader(header)
	stream.SetTrailer(trailer)
	stream.SendHeader(&para)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			trailer.Set("stream-trailer-b", "bbb")
			trailer.Set("stream-trailer-x", "another")
			stream.SetTrailer(trailer)
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(&streamResp{"response -> " + req.str}); err != nil {
			return err
		}
	}
}

func runServer() {
	cmd.Main(os.Args)
}
