module github.com/cubefs/cubefs/blobstore

go 1.15

replace github.com/desertbit/grumble v1.1.1 => github.com/sejust/grumble v1.1.2-0.20210930091007-2c8622e565d3

require (
	github.com/Shopify/sarama v1.22.1
	github.com/afex/hystrix-go v0.0.0-20180502004556-fa1af6a1f4f5
	github.com/alicebob/miniredis/v2 v2.16.1
	github.com/benbjohnson/clock v1.3.0
	github.com/deniswernert/go-fstab v0.0.0-20141204152952-eb4090f26517
	github.com/desertbit/grumble v1.1.1
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.13.0
	github.com/globalsign/mgo v0.0.0-20181015135952-eeefdecb41b8
	github.com/go-redis/redis/v8 v8.11.4
	github.com/golang/mock v1.6.0
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/consul/api v1.11.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/julienschmidt/httprouter v1.3.0
	github.com/klauspost/reedsolomon v1.9.2
	github.com/opentracing/opentracing-go v1.2.0
	github.com/prometheus/client_golang v1.11.0
	github.com/stretchr/testify v1.7.0
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	go.etcd.io/etcd/raft/v3 v3.5.1
	go.mongodb.org/mongo-driver v1.1.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11
	gopkg.in/go-playground/validator.v9 v9.31.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/BurntSushi/toml v0.4.1 // indirect
	github.com/facebookgo/ensure v0.0.0-20200202191622-63f1cf65ac4c // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20200203212716-c811ad88dec4 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/klauspost/cpuid v1.3.1 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.17.0 // indirect
	github.com/smartystreets/goconvey v1.7.2 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	gopkg.in/go-playground/assert.v1 v1.2.1 // indirect
)
