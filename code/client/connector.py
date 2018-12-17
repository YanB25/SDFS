import rpyc
import os
import traceback

class Connector():
    def __init__(self, ip=None, port=None):
        '''
        @param (ip, port) namenode.
        '''
        if ip is None and port is None:
            address = rpyc.discover('NAMENODE')[0]
            self.ip, self.port = address
        else:
            assert(ip is not None and port is not None)
            self.ip = ip
            self.port = port
    def put(self, filename, blk_sz=16384, replica=3):
        '''
        将文件上传到DataNode。
        @detail
            会分为若干个步骤。首先调用NameNode的put函数，请求这份文件块的分配信息。
            该函数会返回文件块都分配在哪些DataNode上。

            然后对每个DataNode调用put_block，将文件块实际发送到各个DataNode上。
            注意，在做put_block之前，要先调用NameNode的put_block_register,以便
            NameNode统计结果。
        @param filename :: Str, 上传的文件名
        @param blk_sz(Optional) :: Int, 块大小
        @param replica(Optional) :: Int, 副本个数

        @ret Tuple(Boolean, Str), 是否成功，提示信息
        '''
        with open(filename, 'rb') as f:
            binary = f.read()
            size = len(binary)
            namenode_conn = rpyc.connect(self.ip, self.port)
            errno, datanodes = namenode_conn.root.put(filename, size, blocksize=blk_sz, replica=replica)
            if errno == 1:
                return False, 'Not enough DataNode for replica'

            # 告知namenode，并对datanode作IO
            for idx, segment in enumerate(datanodes):
                if (idx + 1) * blk_sz <= size:
                    write_binary = binary[idx * blk_sz: (idx + 1) * blk_sz]
                else:
                    write_binary = binary[idx * blk_sz :]
                for datanode in segment:
                    ip, port = datanode
                    conn = rpyc.connect(ip, port)
                    # 将该块传给datanode
                    namenode_conn.root.put_block_registry(filename, idx, datanode)
                    errno, msg = conn.root.put_block(filename, idx, write_binary)
                    if errno == 1:
                        print('Warning: {}-{} alread exists {} block {}'.format(ip, port, filename, idx))
        return True, 'success'
    def cat(self, filename):
        '''
        根据文件名获得文件块
        @param filename
        '''
        namenode_conn = rpyc.connect(self.ip, self.port)
        errno, datanodes = namenode_conn.root.get(filename)
        if errno == 1:
            return False, 'No file {} found'.format(filename)
        if errno == 2:
            return False, 'Could not find enough healthy replica.\nuse python sdfs.py rm {} to remove this file'.format(filename)
        if errno == 3:
            return False, 'Not enough good replica left. \nuse python sdfs.py rm {} to remove this file'.format(filename)
        
        file = b''
        for blockid, datanode in enumerate(datanodes):
            ip, port = datanode
            conn = rpyc.connect(ip, port)
            errno, binary = conn.root.get_block(filename, blockid)
            if errno == 1:
                return False, 'Unknown error when getting file {}, block not exists. Abort'.format(filename)
            file += binary
        try:
            output = file.decode('ascii')
            print(output)
        except:
            print(file)
        return True, 'success'

    def get(self, filename, dst_filename=None, force=False):
        '''
        根据文件名获得文件块
        @param filename
        '''
        if dst_filename is None:
            dst_filename = filename
        if os.path.exists(dst_filename) and not force:
            return False, 'file {} exists. Abort.\nTry --force'.format(filename)

        namenode_conn = rpyc.connect(self.ip, self.port)
        errno, datanodes = namenode_conn.root.get(filename)
        if errno == 1:
            return False, 'No file {} found'.format(filename)
        if errno == 2:
            return False, 'Could not find enough healthy replica.\nuse python sdfs.py rm {} to remove this file'.format(filename)
        if errno == 3:
            return False, 'Not enough good replica left. \nuse python sdfs.py rm {} to remove this file'.format(filename)
        
        file = b''
        for blockid, datanode in enumerate(datanodes):
            ip, port = datanode
            conn = rpyc.connect(ip, port)
            errno, binary = conn.root.get_block(filename, blockid)
            if errno == 1:
                return False, 'Unknown error when getting file {}, block not exists. Abort'.format(filename)
            file += binary
        with open(dst_filename, 'wb') as f:
            f.write(file)
        return True, 'success'
    def rm(self, filename):
        '''
        根据文件名删除文件
        @param filename :: Str
        @ret Boolean, Str
        '''
        namenode_conn = rpyc.connect(self.ip, self.port)
        errno, config_file = namenode_conn.root.ownfile(filename)
        if errno == 1:
            return False, 'No file {} found'.format(filename)
        # print(config_file)
        for blockid in config_file:
            datanodes = config_file[blockid]
            for datanode in datanodes:
                ip = datanode['ip']
                port = datanode['port']
                try:
                    conn = rpyc.connect(ip, port)
                    errno = conn.root.rm_block(filename, blockid)
                    if errno == 1:
                        print('WARNING, {} block {} not found at {}:{}'.format(filename, blockid, ip, port))
                except:
                    print('node {}:{} temporary disconnect'.format(ip, port))
                    traceback.print_exc()
        errno = namenode_conn.root.rm_register(filename)
        if errno == 1:
            print('WARNING: {} not found'.format(filename))
        return True, 'success'
    
    def ls(self, all=False):
        '''
        列出存储着的文件
        '''
        namenode_conn = rpyc.connect(self.ip, self.port)
        if all:
            error, msg = namenode_conn.root.fresh_update()
            if error == 0:
                fmt = '\t{:<30}{:<10}{}/{}'
                print(fmt.format('filename', 'block', 'valid', 'replica'))
                for filename in msg:
                    sums = 0
                    valid = 0
                    for blkid in msg[filename]:
                        replica_list = msg[filename][blkid]
                        valid += sum([1 if item['healthy'] else 0 for item in replica_list])
                        sums += len(replica_list)
                    print(fmt.format(filename, len(msg[filename]), valid, sums))
                print('run python sdfs.py autofix to fix the error replica if valid != replica')
                return True, 'success'
            else:
                return False, 'error'
        else:
            errno, config = namenode_conn.root.ls()
            if errno == 1:
                return False, 'error'
            fmt='\t{:<30}{:<10}'
            print(fmt.format('filename', 'block'))
            for file_info in config:
                print(fmt.format(file_info['filename'], file_info['replica']))
            return True, 'success'
    def node(self):
        namenode_conn = rpyc.connect(self.ip, self.port)
        return namenode_conn.root.ping_all()
            


if __name__ == '__main__':
    connector = Connector()
    # print(connector.put('data-Trie.png'))
    print(connector.get('data-Trie.png', 'data-Trie3.png'))
