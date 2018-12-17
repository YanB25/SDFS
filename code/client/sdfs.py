import sys
from parser import parser
from connector import Connector


class Dispatcher:
    def __init__(self):
        self.parser = parser
        self.conn = Connector()
    def parse(self):
        if len(sys.argv[1:]) == 0:
            parser.print_help()
        self.args = parser.parse_args()
        self.dispatch()
    def dispatch(self):
        if self.args.which == 'put':
            success, msg = self.conn.put(self.args.file)
            if not success:
                self.args.print_help()
            print(msg)
        if self.args.which == 'get':
            if self.args.dst == '':
                success, msg = self.conn.get(self.args.file)
            else:
                success, msg = self.conn.get(self.args.file, self.args.dst)
            if not success:
                self.args.print_help()
            print(msg)
        # if self.args.which == 'ls':

def main():
    dispatcher = Dispatcher()
    dispatcher.parse()

if __name__ == '__main__':
    main()

    