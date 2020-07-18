package thunderpb

//go:generate sh -c "docker run -v `pwd`:/defs namely/protoc-all:1.11 -d . -l gogo && mv gen/pb-gogo/github.com/samsarahq/thunder/thunderpb/* . && rm -rf gen"
