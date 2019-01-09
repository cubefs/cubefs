## getPartitions
```bash
curl -v  http://127.0.0.1:9092/getPartitions
```
Get all meta-partition base information of the metanode.

##getPartitionById
```bash
curl -v http://127.0.0.1:9092/getPartitionById?pid=100
```
Get the specified parition information, this result cnotains: leader address, raft group peer and cursor.
    
|parameter | type | desc|
|:---:|:---:|:---:|
| pid |integer |meta-partition id |
    
    
    
