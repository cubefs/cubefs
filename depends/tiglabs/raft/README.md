# Raft

A multi-raft implementation built on top of the [CoreOS etcd raft library](https://github.com/etcd-io/etcd). 

## Installation

Download and install to `GOPATH`:
```bash
go get -u github.com/cubefs/cubefs/depends/tiglabs/raft
```

## Features

The CoreOS etc/raft implementation has been modified to add the following features.

- multi-raft support    
- snapshot manager   
- merged and compressed heartbeat message    
- check down replica      
- single raft's panic is allowed, detectable  
- new wal implementation    
- export more run status    
- implementation batch commit

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). 
For detail see [LICENSE](LICENSE) and [NOTICE](NOTICE).
