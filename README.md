# CrushStore

A horizontally scaling object store based on the CRUSH placement algorithm.

## Use cases and limitations

CrushStore is designed to be an operationally simple ho


- Save an object associated with a key.
- Get an object by key.
- Delete an object associated with a key.
- List all keys.

## Limitations

### Key/Value immutability

Each key must have a unique value - e.g. a uuid or some other unique identifier tied to a immutable data. This restriction allows uploads to be idempotent and simplifies the system.

### No ordered listing

There is no global ordering 
 

# Getting started

## Building

```
$ git clone https://github.com/andrewchambers/crushstore
$ cd crushstore
$ go build ./cmd/...
$ mkdir bin
$ cp  $(find ./cmd -type f -executable) ./bin
$ ls ./bin
crushstore
crushstore-delete
crushstore-get
crushstore-list
crushstore-list-keys
crushstore-put
```

## Running a simple cluster

Create a test config - crushstore-cluster.conf:
```
cluster-secret: password
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

Change a 'healthy' line in the config to 'defunct' and wait for crushstore to reload the config:
```
...
storage-nodes:
    - 100 healthy http://127.0.0.1:5000
    - 100 defunct http://127.0.0.1:5001
    - 100 healthy http://127.0.0.1:5002
```

The crushstore scrubber job will rebalance data to maintain the desired placement rule.