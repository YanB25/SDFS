import rpyc
import os

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
    def put(self, filename):
        '''
        将文件上传到DataNode。
        @detail
            会分为若干个步骤。首先调用NameNode的put函数，请求这份文件块的分配信息。
            该函数会返回文件块都分配在哪些DataNode上。

            然后对每个DataNode调用put_block，将文件块实际发送到各个DataNode上。
            注意，在做put_block之前，要先调用NameNode的put_block_register,以便
            NameNode统计结果。
        @param filename :: Str, 上传的文件名

        @ret Tuple(Boolean, Str), 是否成功，提示信息
        '''
        with open(filename, 'rb') as f:
            binary = f.read()
            size = len(binary)
            namenode_conn = rpyc.connect(self.ip, self.port)
            blk_sz = 16384
            errno, datanodes = namenode_conn.root.put(filename, size, blocksize=blk_sz)
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
        return True
    def get(self, filename, dst_filename=None):
        if dst_filename is None:
            dst_filename = filename
        if os.path.exists(dst_filename):
            return False, 'file {} exists. Abort.\n Try --force'

        namenode_conn = rpyc.connect(self.ip, self.port)
        errno, datanodes = namenode_conn.root.get(filename)
        if errno == 1:
            return False, 'No file {} found'.format(filename)
        
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

if __name__ == '__main__':
    connector = Connector()
    print(connector.put('data-Trie.png'))
    print(connector.get('data-Trie.png', 'data-Trie3.png'))
