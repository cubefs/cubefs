# Access Management

## Service Status

```bash
curl http://127.0.0.1:9500/access/status
```

**Response Example**

```json
{
  "clusters": [
    {
      "available": 269036751421440,
      "capacity": 508182930538496,
      "cluster_id": 1,
      "nodes": [ "..." ],
      "readonly": false,
      "region": "test-region"
    }
  ],
  "config": {
    "cluster_config": {
      "...": "...",
      "clusters": null,
      "consul_agent_addr": "127.0.0.1:8500",
      "region": "test-region",
      "region_magic": "test-region"
    }
  },
  "limit": {
    "read_wait": 0,
    "running": {},
    "write_wait": 0
  },
  "pool": [
    {
      "capacity": -1,
      "idle": 0,
      "running": 0,
      "size": 16384
    }
  ],
  "services": {
    "1": {
      "PROXY": [ "..." ]
    }
  }
}
```
