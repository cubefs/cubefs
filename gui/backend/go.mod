module github.com/cubefs/cubefs/console/backend

go 1.16

require (
	github.com/aws/aws-sdk-go v1.33.1
	github.com/cubefs/blobstore v0.0.0-00010101000000-000000000000
	github.com/cubefs/cubefs v1.5.2-0.20230329004538-3212a6348599
	github.com/cubefs/cubefs/blobstore v0.0.0-20230329004538-3212a6348599
	github.com/gin-contrib/sessions v0.0.5
	github.com/gin-gonic/gin v1.7.7
	github.com/globalsign/mgo v0.0.0-20181015135952-eeefdecb41b8
	github.com/go-gormigrate/gormigrate/v2 v2.0.2
	github.com/jinzhu/copier v0.3.5
	github.com/spf13/viper v1.11.0
	google.golang.org/protobuf v1.30.0 // indirect
	gorm.io/driver/mysql v1.5.0
	gorm.io/gorm v1.25.0
	gorm.io/plugin/dbresolver v1.4.1
)

replace (
	code.google.com/p/go.net/context => ../depends/code.google.com/p/go.net/context
	github.com/cubefs/blobstore => github.com/cubefs/cubefs-blobstore v0.0.0-20220126103542-48f2f9535cc8
	github.com/jacobsa/daemonize => ../../depends/jacobsa/daemonize
	github.com/spf13/cobra => ../../depends/spf13/cobra
)
