# Nodeset Management

## List NodeSets

```bash
cfs-cli nodeset list
```

## Show NodeSet

```bash
cfs-cli nodeset info [NODESET ID]
```

## Update NodeSet

```bash
cfs-cli nodeset update [NODESET ID] [flags]
```

```bash
Flags:
    --dataNodeSelector string   Set the node select policy(datanode) for specify nodeset
    -h, --help                      help for update
    --metaNodeSelector string   Set the node select policy(metanode) for specify nodeset
```