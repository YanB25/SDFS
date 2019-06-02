import rpyc
import sys
import os
import hashlib
assert(len(sys.argv) == 2)
PORT = int(sys.argv[1])

print(rpyc.__version__)

class DataNodeService(rpyc.Service):
    def __init__(self, storage_path=None):
        if storage_path is None:
            self.storage_path = 'data-{}/'.format(str(PORT))
        else:
            self.storage_path = storage_path
    def on_connect(self, conn):
        print('OPEN - {}'.format(conn))
    def on_disconnect(self, conn):
        print('COLSE - {}'.format(conn))
    def exposed_put_block(self, filename, blockid, binary, force=False):
        '''
        将block存储到本地文件中
        @param filename :: Str, 存储的文件名
        @param blockid :: Int， 第blockid块
        @param binary :: bStr, 待写入的数据

        @ret Tuple(Int, Str) :: errno and message
            - 0: no error
        '''
        if not os.path.isdir(self.storage_path):
            os.system('mkdir {}'.format(self.storage_path))
        target_filename = '{}/{}-{}'.format(self.storage_path, filename, str(blockid))
        if os.path.exists(target_filename):
            return 1, 'block already exists'
        with open(target_filename, 'wb') as f:
            f.write(binary)
        return 0, 'ok'
    def exposed_get_block(self, filename, blockid, method='normal'):
        '''
        返回文件filename的第blockid块
        @param filename :: Str
        @param blockid :: Int

        @ret Tuple(Int, bStr), 返回第一个参数是错误码。
            - 0：无错误，
            - 1: 块不存在
            返回第二个参数是块的二进制数据
        '''
        if (method not in ['normal', 'md5']):
            return 2, b''

        target_filename = '{}/{}-{}'.format(self.storage_path, filename, str(blockid))
        if not os.path.exists(target_filename):
            return 1, b''
        with open(target_filename, 'rb') as f:
            binary = f.read()
            if (method == 'normal'):
                return 0, binary
            elif (method == 'md5'):
                md5 = hashlib.md5()
                md5.update(binary)
                return 0, md5.hexdigest()
    def exposed_rm_block(self, filename, blockid):
        '''
        @ret Int, 错误码
            - 0: no error
            - 1: block not found
        '''
        target_filename = '{}/{}-{}'.format(self.storage_path, filename, str(blockid))
        if not os.path.exists(target_filename):
            return 1
        os.system('rm {}'.format(target_filename))
        return 0

        

if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(DataNodeService, port=PORT, protocol_config={
        'allow_public_attrs': True,
    }, auto_register=True)
    print('start at port {}'.format(PORT))
    t.start()