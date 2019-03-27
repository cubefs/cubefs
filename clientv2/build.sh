go build -ldflags "-X main.Version=`git rev-parse HEAD`" -o client
