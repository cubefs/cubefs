// Copyright 2022 The CubeFS Authors.
// application memory data controller and metric exporter in localhost.
//
// With tag `noprofile` to avoid generating scripts.
//
// Note:
//   1. You can register http handler to profile multiplexer to control.
//   2. You may use environment to set variables, like `bind_addr`, `metric_exporter_labels`.
//   3. You cannot access the localhost service before profile initialized.
//
// Example:
//
// package main
//
// import (
//     "net/http"
//
//     // 1. you can using default handler in profile defined
//     // 2. you can register handler to profile in other module
//     "github.com/cubefs/cubefs/blobstore/common/profile"
//	   "github.com/cubefs/cubefs/blobstore/common/rpc"
//
// )
//
// func registerHandler() {
//     profile.HandleFunc("GET","/myself/controller", func(ctx *rpc.Context) {
//         // todo something
//     }, rpc.ServerOption...)
// }
//
// func main() {
//  	ph := profile.NewProfileHandler("127.0.0.1:8888")
//		httpServer := &http.Server{
//			Addr:    "127.0.0.1:8888",
//			Handler: rpc.MiddlewareHandlerWith(rpc.DefaultRouter, ph),
//		}
//		log.Info("Server is running at", "127.0.0.1:8888")
//		go func() {
//			err = httpServer.ListenAndServe()
//			require.NoError(t, err)
//		}()
//      registerHandler()
// }
package profile
