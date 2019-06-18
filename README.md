# Simple Distributed FileSystem
[click here](https://github.com/YanB25/SDFS/blob/master/report/16337269_%E9%A2%9C%E5%BD%AC.pdf) for the report of this project.
## requirement
- tmux installed
- Python >= 3.5
- rpyc (python package) installed
## How to run
### Server
``` shell
$ cd /code/server
# run `name node` and `data node` via tmux.
$ bash run.sh
```
### Registry
``` shell
$ python rpyc_registry.py 
```
### Client
``` shell
$ cd code/client
$ python sdfs.py -h
```
See the help for how to use the it.
