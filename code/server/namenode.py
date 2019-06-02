'''
TODO: 有个问题，多线程同时读、写self.storage_track文件，要加锁。
TODO: put有时会返回掉线的peer，怎么办呢
'''
import traceback
import copy
import rpyc
import sys
import os
import math
import random
import pickle
import pprint
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
        self.conns = []
        self.datanodes = []
        datanode_candidate = rpyc.discover('DATANODE')
        for node in datanode_candidate:
            ip, port = node
            try:
                conn = rpyc.connect(ip, port)
                self.conns.append(conn)
                self.datanodes.append((ip, port))
            except:
                pass
    def on_disconnect(self, conn):
        print('COLSE - {}'.format(conn))
        for conn in self.conns:
            conn.close()
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
        datanodes = self.datanodes
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
                self.tracking[filename][partid].append({
                    'ip': datanode[0],
                    'port': datanode[1],
                    'healthy': True
                })
            else:
                self.tracking[filename][partid] = [{
                    'ip': datanode[0],
                    'port': datanode[1],
                    'healthy': True
                }]
        else:
            self.tracking[filename] = {
                partid: [{
                    'ip': datanode[0],
                    'port': datanode[1],
                    'healthy': True
                }]
            }
        print(self.tracking)
        self.__store_tracking()

        return 0
    def exposed_get(self, filename):
        '''
        @ret Int, List[Tuple(Str, Int)], 第一个返回值是错误码，无错误返回0.
            第二个返回值是DataNode的列表。
            - 0: noerror
            - 3: not enough good replica
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
                ip = datanode['ip']
                port = datanode['port']
                healthy = datanode['healthy']
                if not healthy:
                    print('not healthy')
                    continue
                try:
                    conn = rpyc.connect(ip, port)
                    conn.close()
                    ret_config.append((ip, port))
                    hasNode = True
                    break
                except:
                    print('{}-{} not exist, continue')
            if not hasNode:
                return 2, []
        if len(ret_config) != len(self.tracking[filename]):
            return 3, []
        pprint.pprint(self.tracking)
        pprint.pprint(ret_config)
        return 0, ret_config
    def exposed_ownfile(self, filename):
        '''
        返回拥有这些文件DataNode
        @param filename:: Str
        @ret Tuple(Int, [[(Str, Int)]]) 一系列地址
        '''
        self.__load_tracking()
        if not self.tracking.get(filename):
            return 1, []
        config_file = self.tracking[filename]
        return 0, config_file
    def exposed_ls(self):
        '''
        @ret Int, {}
        '''
        self.__load_tracking()
        ret = []
        for filename in self.tracking:        
            ret.append({
                'filename': filename,
                'block': len(self.tracking[filename])
            })
        return 0, ret
    def exposed_rm_register(self, filename):
        self.__load_tracking()
        if not self.tracking.get(filename):
            return 1
            
        self.tracking = {key: self.tracking[key] for key in self.tracking if key != filename}
        self.__store_tracking()
        return 0
    def exposed_fresh_update(self, limited_filename=''):
        '''
        向所有建立了连接的结点询问所有的存储块，比较确定哪些块是坏块。
        @param limited_filename (Option) :: Str. 如果limited_filename是'',则更新所有的filename，否则，仅更新filename指定的文件
        @ret Int, 错误码
            - 0: no error
        '''
        #TODO:
        self.__load_tracking()

        self.new_tracking = {}
        self.checking = {}

        datanodes = self.datanodes
        for filename in self.tracking:
            if limited_filename != '' and filename != limited_filename:
                continue
            for blockid in self.tracking[filename]:
                datanodes = self.tracking[filename][blockid]
                for datanode in datanodes:
                    ip = datanode['ip']
                    port = datanode['port']
                    try:
                        conn = rpyc.connect(ip, port)
                        errno, binary = conn.root.get_block(filename, blockid, method='md5')
                        if not self.checking.get((filename, blockid)):
                            self.checking[(filename, blockid)] = [{
                                'binary': binary,
                                'ip': ip,
                                'port': port
                            }]
                        else:
                            self.checking[(filename, blockid)].append({
                                'binary': binary,
                                'ip': ip,
                                'port': port
                            })
                    except:
                        traceback.print_exc()
        print('checking:', self.checking)
        for key, vals in self.checking.items():
            filename, blockid = key

            binary_list = [item['binary'] for item in vals]
            print('binary_list', binary_list)

            val, cnt = NameNodeService.count(binary_list)
            idx = NameNodeService.uargmax(cnt)
            max_count = max(cnt)
            if sum([1 if item == max_count else 0 for item in cnt]) >= 2:
                correct_binary = b''
            else:
                correct_binary = binary_list[idx]
            for item in vals:
                b = item['binary']
                ip = item['ip']
                port = item['port']
                if b != correct_binary:
                    try:
                        idx = self.tracking[filename][blockid].index({
                            'ip': ip,
                            'port': port,
                            'healthy': True
                        })
                        self.tracking[filename][blockid][idx]['healthy'] = False
                    except:
                        traceback.print_exc()
        print(self.tracking)
        self.__store_tracking()
        return 0, self.tracking
    def exposed_ping_all(self):
        datanodes = self.datanodes
        ups = []
        downs = []
        for datanode in datanodes:
            ip, port = datanode
            try:
                conn = rpyc.connect(ip, port)
                conn.close()
                ups.append((ip, port))
            except:
                downs.append((ip, port))
        return 0, {
            'up': ups,
            'down': downs
        }

    @staticmethod
    def count(ls):
        ''' val, cnt'''
        d = {}
        for item in ls:
            if d.get(item):
                d[item] += 1
            else:
                d[item] = 1
        keys = list(d.keys())
        vals = [d[key] for key in keys]
        return keys, vals
    @staticmethod
    def uargmax(ls):
        assert(len(ls) != 0)
        maxval = -1
        i = -1
        for idx, item in enumerate(ls):
            if item > maxval:
                maxval = item
                i = idx
        return i

        









if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(NameNodeService, port=PORT, protocol_config={
        'allow_public_attrs': True,
    }, auto_register=True)
    print('start at port {}'.format(PORT))
    t.start()