# Simple Distributed FileSystem
## requirement
- tmux installed
- rpyc (python package) installed
## Server
### set up default server
``` shell
$ cd /code/server
$ bash run.sh
```
### manually set up server
``` shell
# registry can work when all
# server in the same network
$ python rpyc_registry.py 
```
``` shell
$ python namenode.py # set up name node
```
``` shell
$ python datanode.py # setup data node
```
## Client
``` shell
$ cd code/client
$ python sdfs.py -h
```