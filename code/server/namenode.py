'''
TODO: 有个问题，多线程同时读、写self.storage_track文件，要加锁。
TODO: 改一下众多函数的返回值
'''
import rpyc
import sys
import os
import math
import random
import pickle
assert(len(sys.argv) == 2)
PORT = int(sys.argv[1])

print(rpyc.__version__)

class NameNodeService(rpyc.Service):
    def __init__(self, storage_path=None):
        if storage_path is None:
            self.storage_path = 'namenode-data-{}/'.format(PORT)
        else:
            self.storage_path = storage_path
        self.tracking = {}
        self.storage_tracking = self.storage_path + 'tracking'
        
        if not os.path.isdir(self.storage_path):
            os.system('mkdir {}'.format(self.storage_path))
        if not os.path.exists(self.storage_tracking):
            os.system('touch {}'.format(self.storage_tracking))
            f = open(self.storage_tracking, 'wb')
            pickle.dump(self.tracking, f)
            f.close()

    def on_connect(self, conn):
        print('OPEN - {}'.format(conn))
    def on_disconnect(self, conn):
        print('COLSE - {}'.format(conn))
    def exposed_put(self, filename, filesize, replica=3, blocksize=1024):
        '''
        NameNode回应存放文件的方式
        @param filename :: Str 文件名
        @param filesize :: Int 文件字节数
        @param replica :: Int, 每个块的副本数
        @param blocksize :: Int, 每个块的大小
        @ret Int, [[Tuple(Str, Int)]], 
            第一个参数是错误码
                - 0: no error
                - 1: not enough DataNode
        
            第一维度是block对应的一系列namenode。第二维度是这些namenode的ip和port。
        '''
        ret = []
        datanodes = rpyc.discover('DATANODE')
        if len(datanodes) < replica:
            return 1, []
        segments = math.ceil(filesize / blocksize)
        available_idx = [i for i in range(len(datanodes))]
        for sec in range(segments):
            random.shuffle(available_idx)
            chosen_idx = available_idx[:replica]
            ret.append([datanodes[i] for i in chosen_idx])
        return 0, ret
    def __load_tracking(self):
        f = open(self.storage_tracking, 'rb')
        self.tracking = pickle.load(f)
        f.close()
    def __store_tracking(self):
        f = open(self.storage_tracking, 'wb')
        pickle.dump(self.tracking, f)
        f.close()
    def exposed_put_block_registry(self, filename, partid, datanode):
        '''
        注册函数，在client和datanode传输文件块前调用
        @param filename :: Str
        @param partid :: Int, 第partid块
        @param datanode :: Tuple(Str, Int), datanode的address

        @ret Int, whether succeed
        '''
        self.__load_tracking()

        if self.tracking.get(filename):
            if self.tracking[filename].get(partid):
                self.tracking[filename][partid].append(datanode)
            else:
                self.tracking[filename][partid] = [datanode]
        else:
            self.tracking[filename] = {
                partid: [datanode]
            }
        print(self.tracking)
        self.__store_tracking()

        return 0
    def exposed_get(self, filename):
        '''
        @ret Int, List[Tuple(Str, Int)], 第一个返回值是错误码，无错误返回0.
            第二个返回值是DataNode的列表。
        '''
        self.__load_tracking()

        if not self.tracking.get(filename):
            print(self.tracking)
            return 1, []
        file_config = self.tracking[filename]
        ret_config = []
        for blockid in file_config:
            datanodes = file_config[blockid]
            hasNode = False
            for datanode in datanodes:
                ip, port = datanode
                try:
                    conn = rpyc.connect(ip, port)
                    conn.close()
                    ret_config.append(datanode)
                    hasNode = True
                    break
                except:
                    print('{}-{} not exist, continue')
            if not hasNode:
                return 2, []

        return 0, ret_config




if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(NameNodeService, port=PORT, protocol_config={
        'allow_public_attrs': True,
    }, auto_register=True)
    print('start at port {}'.format(PORT))
    t.start()