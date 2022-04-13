module github.com/cubefs/cubefs

go 1.16

replace (
	github.com/cubefs/blobstore => github.com/cubefs/cubefs-blobstore v0.0.0-20220126103542-48f2f9535cc8
	github.com/jacobsa/daemonize => ./depends/jacobsa/daemonize
	github.com/jacobsa/fuse => ./depends/jacobsa/fuse
	github.com/spf13/cobra => ./depends/spf13/cobra
	github.com/tecbot/gorocksdb => github.com/tecbot/gorocksdb v0.0.0-20171109104638-15d543158317
)

require (
	github.com/aws/aws-sdk-go v1.33.1
	github.com/bits-and-blooms/bitset v1.2.1
	github.com/brahma-adshonor/gohook v1.1.9
	github.com/cubefs/blobstore v0.0.0-00010101000000-000000000000
	github.com/edsrzf/mmap-go v1.1.0
	github.com/google/btree v1.0.1
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/graphql-go/graphql v0.8.0 // indirect
	github.com/hashicorp/consul/api v1.12.0
	github.com/jacobsa/daemonize v0.0.0-00010101000000-000000000000
	github.com/prometheus/client_golang v1.11.0
	github.com/samsarahq/thunder v0.0.0-20211005041752-96f4331b7baa
	github.com/shurcooL/httpfs v0.0.0-20190707220628-8d4bc4ba7749 // indirect
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546
	github.com/spf13/cobra v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.7.0
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	github.com/xtaci/smux v1.5.16
	golang.org/x/net v0.0.0-20210813160813-60bc85c4be6d
	golang.org/x/sys v0.0.0-20220209214540-3681064d5158
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11
	gopkg.in/yaml.v2 v2.4.0
)
