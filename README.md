# CrushStore

A horizontally scaling object store based on the CRUSH placement algorithm.

## How it works

All clients and nodes in the storage cluster have a copy of the cluster configuration,
when a request for an object arrives, we are able to locate the subset of servers it 
should be stored on without network communication - conceptually it is similar
to a distributed hash table lookup.

Periodically (and after configuration changes), all storage nodes will 'scrub' their data
directory looking for corrupt objects and ensuring the storage placement requirements are met,
replicating data to other nodes if they are not.

If storage nodes disagree on the current cluster configuration, replication requests will be rejected and
retried until the configurations are in agreement again.
It is the responsibility of the administrator to keep the cluster configurations consistent in a timely way
(This can be done with NFS/rsync+cron/git + cron/watching a consul key/...).


## Use cases and limitations

CrushStore is designed to be an operationally simple object store that you use 
as part of a larger storage system. Generally you would store keys in some other
database and use this to lookup items.

Like a hash table, it supports the following operations:

- Save an object associated with a key.
- Get an object associated with a given key.
- Delete an object associated with a key.
- Unordered listing of keys.

Unlike a hash table, CrushStore has an additional constraint:

Writes to a key are eventually consistent - upload conflicts are resolved by the create timestamp
and reading from a different server you just wrote to may return a different value if
the correct value has not been replicated.

# Getting started

## Building

```
$ git clone https://github.com/andrewchambers/crushstore
$ cd crushstore
$ go build ./cmd/...
$ mkdir bin
$ cp $(find ./cmd -type f -executable) ./bin
$ ls ./bin
crushstore
crushstore-cluster-status
crushstore-delete
crushstore-get
crushstore-head
crushstore-list
crushstore-list-keys
crushstore-put
```

## Running a simple cluster

Create a test config - crushstore-cluster.conf:
```
storage-schema: host
placement-rules:
    - select host 2
storage-nodes:
    - 100 healthy http://127.0.0.1:5000
    - 100 healthy http://127.0.0.1:5001
    - 100 healthy http://127.0.0.1:5002
```

Create and run three instances of crushstore:

```
$ mkdir data0
$ ./crushstore -listen-address 127.0.0.1:5000 -data-dir ./data0
```

```
$ mkdir data1
$ ./crushstore -listen-address 127.0.0.1:5001 -data-dir ./data1
```

```
$ mkdir data2
$ ./crushstore -listen-address 127.0.0.1:5002 -data-dir ./data2
```

Upload objects:

```
$ echo hello |  curl -L -F data=@- 'http://127.0.0.1:5000/put?key=testkey1'
echo hello | ./bin/crushstore-put testkey2 -
```

Download objects:

```
$ curl -L 'http://127.0.0.1:5000/get?key=testkey1' -o -
$ ./bin/crushstore-get testkey2
```

List objects:

```
$ ./bin/crushstore-list
```

Delete objects:

```
$ curl -L -X POST  'http://127.0.0.1:5001/delete?key=testkey1'
$ ./bin/crushstore-delete testkey2
```

## Experiment with data rebalancing

Create some test keys:
```
for i in $(seq 10)
do
	echo data | ./bin/crushstore-put $(uuidgen) -
done
```

Change a 'healthy' line in the config to 'defunct' and change the relative
weight of one of the storage nodes, then wait for crushstore to reload the config:


```
...
storage-nodes:
    - 50 healthy http://127.0.0.1:5000
    - 100 defunct http://127.0.0.1:5001
    - 100 healthy http://127.0.0.1:5002
```

The crushstore scrubber job will rebalance data to maintain the desired placement rules and weighting.