package router

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/helper/middleware"
	"github.com/gin-gonic/gin"
)

func RunHTTPServer() {
	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", config.Conf.Server.Port),
		Handler:        getHandler(),
		ReadTimeout:    20 * time.Second,
		WriteTimeout:   20 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	go func() {
		// service connections
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	// Wait for interrupt signal to gracefully shutdown the server with
	// a timeout of 5 seconds.
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		log.Fatalf("Server Shutdown:", err)
	}
	log.Println("Server exiting")
}

func getHandler() *gin.Engine {
	// set server mode
	gin.SetMode(enums.GetGinMode(config.Conf.Server.Mode))
	// set log mode
	log.SetOutputLevel(getLoggerLevel(config.Conf.Server.Mode))
	r := gin.New()
	// Global middleware
	r.Use(
		gin.Recovery(),
		middleware.Cors(),
		middleware.Logger(),
		middleware.InitSession(),
		middleware.Authorization,
		middleware.Default(),
		middleware.Session(),
		middleware.RecordOpLog(),
	)
	// init router
	Register(r)
	//helper.Must(auth.InitAuth())
	return r
}

func getLoggerLevel(mode string) log.Level {
	switch mode {
	case "dev":
		return log.Ldebug
	default:
		return log.Linfo
	}
}
