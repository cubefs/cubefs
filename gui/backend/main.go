package main

import (
	"flag"

	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/model/migrate"
	"github.com/cubefs/cubefs/console/backend/model/mysql"
	"github.com/cubefs/cubefs/console/backend/router"
)

var confPath = flag.String("c", "", "please input your conf file path")

func main() {
	flag.Parse()
	helper.Must(config.Init(*confPath))
	helper.Must(mysql.Init())
	helper.Must(migrate.Init())
	router.RunHTTPServer()
}
