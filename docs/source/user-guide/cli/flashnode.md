# FlashNode Management

## List flash nodes

Obtain detailed information about each cache node in the system, including its unique identifier (ID), network address, and the status of cached data.

```bash
./cfs-cli flashnode list
```

## View FlashNode cache statistics

```bash
// When querying the status of a FlashNode, the associated key does not carry an expiration time.
./cfs-cli flashnode httpStat 127.0.0.1:17510
// When querying the status of a FlashNode, the associated key does carry an expiration time.
./cfs-cli flashnode httpStatAll 127.0.0.1:17510
```

## Evict the volume flash_cache from a specific FlashNode

```bash
./cfs-cli flashnode httpEvict 127.0.0.1:17510 flash_cache
```


## Enable/Disable flashnode

```bash
./cfs-cli flashnode set 127.0.0.1:17510 true
./cfs-cli flashnode set 127.0.0.1:17510 false 
```

## Remove flashnode

```bash
./cfs-cli flashnode remove 127.0.0.1:17510
```

## Create fg

```bash
./cfs-cli flashgroup create
```

## Set flashgroup  active

```bash
./cfs-cli flashgroup set 25 true
```

## Flashgroup add flashnode

```bash
./cfs-cli flashgroup nodeAdd 13 --zone-name=flashcache --addr="127.0.0.1:17510"
```

## List flashgroup

```bash
./cfs-cli flashgroup list
```

## Show flashgroup graph

```bash
./cfs-cli flashgroup graph
```