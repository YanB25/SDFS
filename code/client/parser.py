import argparse
import sys

parser = argparse.ArgumentParser(description='a Simple Distributed FileSystem')

subparsers = parser.add_subparsers()

src_dst_parent_parser = argparse.ArgumentParser(add_help=False)
src_dst_parent_parser.add_argument('src', type=str, help='source file')
src_dst_parent_parser.add_argument('dst', type=str, help='destination file')

recursive_parent_parser = argparse.ArgumentParser(add_help=False)
recursive_parent_parser.add_argument('-r', '--recursive', help='operate recursively', action='store_true')

put_parser = subparsers.add_parser('put', help='put a file into SDFS')
put_parser.add_argument('file', type=str, help='the file you want to upload')
put_parser.set_defaults(which='put')

get_parser = subparsers.add_parser('get', help='get a file from SDFS')
get_parser.add_argument('file', type=str, help='the file you want to get')
get_parser.add_argument('dst', type=str, nargs='?', help='destination path where download to', default='')
get_parser.set_defaults(which='get')

cat_parser = subparsers.add_parser('cat', help='concatenate files and print on the standard output')
cat_parser.add_argument('file', type=str, help='file to be cancatenated or printed')
cat_parser.set_defaults(which='cat')

cp_parser = subparsers.add_parser('cp', 
    help=' copy files and directories', 
    parents=[src_dst_parent_parser, recursive_parent_parser])
cp_parser.set_defaults(which='cp')

mv_parser = subparsers.add_parser('mv', help='move(rename) file', parents=[src_dst_parent_parser])
mv_parser.set_defaults(which='mv')

rm_parser = subparsers.add_parser('rm', help='remove files or directories',
    parents=[recursive_parent_parser])
rm_parser.add_argument('file', help='path to be removed')
rm_parser.set_defaults(which='rm')

ls_parser = subparsers.add_parser('ls', help='list all the file uploaded')
ls_parser.set_defaults(which='ls')

# if len(sys.argv[1:]) == 0:
#     parser.print_help()
# args = parser.parse_args()
# print()
# print(args)