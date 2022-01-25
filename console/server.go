package console

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/service"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/graphql/client"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/introspection"
)

type ConsoleNode struct {
	listen           string
	masters          []string
	objectNodeDomain string
	server           *mux.Router
}

func (c *ConsoleNode) Start(cfg *config.Config) error {
	if err := c.loadConfig(cfg); err != nil {
		return err
	}

	cli := client.NewMasterGClient(c.masters)
	c.addProxy(proto.AdminUserAPI, cli)
	c.addProxy(proto.AdminClusterAPI, cli)
	c.addProxy(proto.AdminVolumeAPI, cli)

	c.server.HandleFunc(proto.ConsoleIQL, cutil.IQLFun)

	loginService := service.NewLoginService(cli)
	c.addHandle(proto.ConsoleLoginAPI, loginService.Schema(), loginService)

	monitorService := service.NewMonitorService(cfg.GetString("monitor_addr"), cfg.GetString("monitor_app"), cfg.GetString("monitor_cluster"), cfg.GetString("dashboard_addr"))
	c.addHandle(proto.ConsoleMonitorAPI, monitorService.Schema(), monitorService)
	c.addHandle("/jiankong", monitorService.Schema(), monitorService)

	fileService := service.NewFileService(c.objectNodeDomain, c.masters, cli)
	c.server.HandleFunc(proto.ConsoleFileDown, func(writer http.ResponseWriter, request *http.Request) {
		if err := fileService.DownFile(writer, request); err != nil {
			c.writeError(err, writer)
		}
	})

	c.server.HandleFunc(proto.ConsoleFileUpload, func(writer http.ResponseWriter, request *http.Request) {
		if err := fileService.UpLoadFile(writer, request); err != nil {
			c.writeError(err, writer)
		}
	})

	c.addHandle(proto.ConsoleFile, fileService.Schema(), fileService)

	indexPaths := []string{"overview", "login", "overview", "userDetails", "servers",
		"serverList", "dashboard", "volumeList", "volumeDetail", "fileList", "operations",
		"alarm", "authorization"}

	for _, path := range indexPaths {
		c.server.HandleFunc("/"+path, c.indexer).Methods("GET")
	}

	c.server.PathPrefix("/").Handler(http.FileServer(Assets))

	return nil
}

func (c *ConsoleNode) indexer(writer http.ResponseWriter, request *http.Request) {
	if file, err := Assets.Open("/index.html"); err != nil {
		c.writeError(err, writer)
	} else {
		defer file.Close()
		if w, e := io.Copy(writer, file); w == 0 && e != nil {
			c.writeError(e, writer)
		}
	}
}

func (c *ConsoleNode) writeError(err error, writer http.ResponseWriter) {
	rep := &proto.GeneralResp{
		Message: err.Error(),
		Code:    http.StatusInternalServerError,
	}
	value, err := json.Marshal(rep)
	if err != nil {
		value = []byte("marshal rep has err")
	}
	if _, err := writer.Write(value); err != nil {
		log.LogErrorf("write has err:[%s]", err.Error())
	}
}

func (c ConsoleNode) Shutdown() {
}

func (c *ConsoleNode) Sync() {
	if err := http.ListenAndServe(fmt.Sprintf(":%s", c.listen), c.server); err != nil {
		log.LogErrorf("sync console has err:[%s]", err.Error())
	}
}

func NewServer() *ConsoleNode {
	return &ConsoleNode{}
}

func (c *ConsoleNode) loadConfig(cfg *config.Config) (err error) {
	// parse listen
	c.listen = cfg.GetString(proto.ListenPort)
	if len(c.listen) == 0 {
		c.listen = "80"
	}
	if match := regexp.MustCompile("^(\\d)+$").MatchString(c.listen); !match {
		return fmt.Errorf("invalid listen configuration:[%s]", c.listen)
	}
	log.LogInfof("console loadConfig: setup config: %v(%v)", proto.ListenPort, c.listen)

	// parse master config
	c.masters = cfg.GetStringSlice(proto.MasterAddr)
	if len(c.masters) == 0 {
		return config.NewIllegalConfigError(proto.MasterAddr)
	}
	log.LogInfof("loadConfig: setup config: %v(%v)", proto.MasterAddr, strings.Join(c.masters, ","))

	c.objectNodeDomain = cfg.GetString(proto.ObjectNodeDomain)

	c.server = mux.NewRouter()

	return
}

func (c *ConsoleNode) addProxy(model string, client *client.MasterGClient) {
	c.server.Handle(model, cutil.NewProxyHandler(client)).Methods("POST")
}

func (c *ConsoleNode) addHandle(model string, schema *graphql.Schema, service interface{}) {
	introspection.AddIntrospectionToSchema(schema)
	c.server.Handle(model, cutil.HTTPHandler(schema)).Methods("POST")
}
