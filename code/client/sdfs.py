import sys
import traceback
from parser import parser
from connector import Connector


class Dispatcher:
    def __init__(self):
        self.parser = parser
        self.conn = Connector()
    def parse(self):
        self.args = parser.parse_args()
        self.dispatch()
    def dispatch(self):
        try:
            if self.args.which == 'put':
                success, msg = self.conn.put(self.args.file)
                if not success:
                    self.parser.print_help()
                print(msg)
            if self.args.which == 'get':
                if self.args.dst == '':
                    success, msg = self.conn.get(self.args.file, force=self.args.force)
                else:
                    success, msg = self.conn.get(self.args.file, self.args.dst, force=self.args.force)
                print(msg)
                if not success:
                    # self.parser.print_help()
                    pass
            # if self.args.which == 'ls':
            if self.args.which == 'rm':
                success, msg = self.conn.rm(self.args.file)
                print(msg)
                if not success:
                    self.parser.print_help()
            if self.args.which == 'ls':
                success, msg = self.conn.ls(all=self.args.all)
                print(msg)
                if not success:
                    self.parser.print_help()
            if self.args.which == 'node':
                success, msg = self.conn.node()
                fmt = '\t{:<20}{:<10}{:<4}'
                print(fmt.format('ip', 'port', 'status'))
                for up_host in msg['up']:
                    ip, port = up_host
                    print(fmt.format(ip, port, 'UP'))
                for up_host in msg['down']:
                    ip, port = up_host
                    print(fmt.format(ip, port, 'DOWN'))
            if self.args.which == 'cat':
                success, msg = self.conn.cat(self.args.file)
                print(msg)
        except AttributeError:
            self.parser.print_help()
        except:
            traceback.print_exc()

def main():
    dispatcher = Dispatcher()
    dispatcher.parse()

if __name__ == '__main__':
    main()

    