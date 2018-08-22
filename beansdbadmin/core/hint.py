#!/usr/bin/env python
# coding:utf-8
''' a item
    = (key, record info, item info)
    = (key, (khash, data_pos, ver, vhash), (i, off_s))
'''

import struct
import sys
import signal
import quicklz
from beansdbadmin.core.hash import get_khash64

ITEM_META_SIZE_OLD = 10
ITEM_META_SIZE_NEW = 23
FILE_HEADER_SIZE_NEW = 16


class HintIndex(object):
    '''key: (version, pos)'''

    def __init__(self):
        pass

    def load(self, path):
        pass

    def loads(self, hint_data):
        pass


def parse_old_hint(hint_data):
    header_size = ITEM_META_SIZE_OLD
    hint_len = len(hint_data)

    off_s = 0
    i = 0
    while off_s < hint_len:
        i += 1
        header = hint_data[off_s:off_s + header_size]
        data_pos, ver, vhash = struct.unpack('IiH', header)
        ksz = data_pos & 0xff
        data_pos -= ksz
        off_s += header_size
        key_ = hint_data[off_s:off_s + ksz]
        yield (key_, (0, data_pos, ver, vhash), (i, off_s))
        off_s += ksz + 1


def parse_new_hint_body(hint_data, check_khash=False):
    '''without header and index'''

    hint_len = len(hint_data)
    i = 0
    off_s = 0
    while off_s < hint_len:
        i += 1
        header = hint_data[off_s:off_s + ITEM_META_SIZE_NEW]
        khash, chunk_id, offset, ver, vhash, ksz = struct.unpack('QiIiHB', header)
        off_s += ITEM_META_SIZE_NEW
        key_ = hint_data[off_s:off_s + ksz]
        yield (key_, (khash, offset, ver, vhash), (i, off_s))
        if check_khash:
            khash2 = get_khash64(key_)
            if khash != khash2:
                print "khash is %016x, should be %016x" % (khash, khash2)
                sys.exit(1)
        off_s += ksz


def parse_new_hint_header(hint_data):
    return struct.unpack('QII', hint_data[:FILE_HEADER_SIZE_NEW])


def parse_new_hint(hint_data, check_khash=False):
    index_off_s, _, _ = parse_new_hint_header(hint_data)
    hint_len = len(hint_data) if index_off_s == 0 else index_off_s
    return parse_new_hint_body(
        hint_data[FILE_HEADER_SIZE_NEW:hint_len],
        check_khash
    )


class HintFile(object):
    def __init__(self, path, is_new=None, stop_on_bad=False, check_khash=False):
        # print check_khash
        self.path = path
        self.stop_on_bad = stop_on_bad
        self.check_khash = check_khash

        path_low = path.lower()
        if is_new in [True, False]:
            self.is_new = is_new
        else:
            if path_low.endswith('.tmp'):
                path_low = path_low[:-4]
            if path_low.endswith('.idx.s'):
                self.is_new = True
            elif path_low.endswith('.hint.qlz'):
                self.is_new = False
            else:
                raise Exception("%s has unexpected suffix" % path)

        with open(self.path, 'r') as f:
            hint_data = f.read()
        if self.is_new:
            index_off_s, count, datasize = parse_new_hint_header(hint_data)
            print "index_off_s, count, datasize: ", index_off_s, count, datasize
            hint_len = len(hint_data) if index_off_s == 0 else index_off_s
            self.g = parse_new_hint_body(
                hint_data[FILE_HEADER_SIZE_NEW:hint_len],
                check_khash
            )
        else:
            hint_data = quicklz.decompress(hint_data)
            self.g = parse_old_hint(hint_data)

    def __iter__(self):
        return self

    def next(self):
        return self.g.next()


def get_keyinfo_from_hint(file_path, key):
    '''return whethor key is in file'''
    hf = HintFile(file_path, check_khash=False)

    for it in hf:
        if it[0] == key:
            _, pos, ver, vhash = it[1]
            return pos & 0xffffff00, ver, vhash
    return None


def format_meta(khash, pos, version, vhash):
    return "%016x %x %d %d" % (khash, pos, version, vhash)


def format_item(key, meta, pos):
    return "%s: %s [%s]" % (pos, format_meta(*meta), key)


def dump_hint():
    import argparse
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    parser = argparse.ArgumentParser(description="dump beansdb data content",
                                     prefix_chars="-")
    parser.add_argument('--new-format', action='store_true')
    parser.add_argument('--old-format', action='store_true')
    parser.add_argument('--check-khash', action='store_true')
    parser.add_argument('hint_file')
    args = parser.parse_args()
    if not args.hint_file:
        print >> sys.stderr, "missing argument hintfile"
        sys.exit(1)
        return
    is_new = None
    if args.new_format:
        is_new = True
    elif args.old_format:
        is_new = False

    print args.check_khash
    hf = HintFile(args.hint_file, is_new, stop_on_bad=True, check_khash=args.check_khash)
    header = "(NO., off): khash(hex) pos(hex) ver vhash [key]"
    print header
    for it in hf:
        print format_item(*it)
    print header


if __name__ == "__main__":
    dump_hint()
