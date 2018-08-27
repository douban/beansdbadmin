#!/usr/bin/env python
# encoding: utf-8
'''only concern data file fomart and value
record = (key, vsz, value, flag, tstamp, ver)
'''

import sys
import struct
import logging
import quicklz
import zlib
import marshal
import binascii
import time

##### consts
### record tuple field indexes

R_KEY = 0
R_VSZ = 1  # before decompress
R_VALUE = 2
R_FLAG = 3
R_TS = 4
R_VER = 5

## flags
FLAG_PICKLE = 0x00000001
FLAG_INTEGER = 0x00000002
FLAG_LONG = 0x00000004
FLAG_BOOL = 0x00000008
FLAG_COMPRESS1 = 0x00000010  # by libmc
FLAG_MARSHAL = 0x00000020
FLAG_COMPRESS = 0x00010000  # by beansdb

## lengths
REC_HEAD_SIZE = 24
PADDING = 256

## settings
MAX_VALUE_SIZE = (100 << 20)
MAX_KEY_LEN = 250


class BadRecord(Exception):
    pass


class SizeError(BadRecord):
    pass


class CRCError(BadRecord):
    pass


#### core functions for parse

def parse_header(block):
    return struct.unpack("IiiiII", block[:REC_HEAD_SIZE])


def get_record_size(ksz, vsz, padding=True):
    rsize = ksz + vsz + REC_HEAD_SIZE
    if padding and rsize & 0xff:
        rsize = ((rsize >> 8) + 1) << 8
    return rsize


def restore_value(flag, val):
    # will ignore pickled
    flag = int(flag)
    if flag & FLAG_COMPRESS:
        val = quicklz.decompress(val)
    if flag & FLAG_COMPRESS1:
        val = zlib.decompress(val)

    if flag & FLAG_BOOL:
        val = bool(int(val))
    elif flag & FLAG_INTEGER:
        val = int(val)
    elif flag & FLAG_MARSHAL:
        val = marshal.loads(val)
    return val


def decompress(val):
    return quicklz.decompress(val)


def parse_record(block):
    '''parse a whole rec'''
    if not block:
        return
    hsz = REC_HEAD_SIZE
    _, tstamp, flag, ver, ksz, vsz = parse_header(block)
    if not 0 < ksz < 255:
        logging.error("wrong ksz %d", ksz)
        return
    if not 0 <= vsz < MAX_VALUE_SIZE:
        logging.error("wrong vsz %d", vsz)
        return
    rsize = hsz + ksz + vsz
    key = block[hsz:hsz + ksz]
    value = block[hsz + ksz:rsize]
    return (key, vsz, value, flag, tstamp, ver)


def parse_records(block, padding=True):
    '''parse whole recs'''
    records = []
    while block:
        r = parse_record(block)
        if r is None:
            break
        rsize = get_record_size(len(r[R_KEY]), r[R_VSZ], padding)
        records.append(r)
        block = block[rsize:]
    return records


def read_record(f, decompress_value=True, check_crc=True):
    '''read a rec from f'''
    block = f.read(PADDING)
    if not block:
        return
    crc, tstamp, flag, ver, ksz, vsz = parse_header(block)
    if not (0 < ksz <= MAX_KEY_LEN) or not 0 <= vsz <= MAX_VALUE_SIZE:
        raise SizeError("size %d %d" % (ksz, vsz))
    rsize = 24 + ksz + vsz
    if rsize & 0xff:
        rsize = ((rsize >> 8) + 1) << 8
    if rsize > PADDING:
        block += f.read(rsize - PADDING)
    if check_crc:
        crc32 = binascii.crc32(block[4:24 + ksz + vsz]) & 0xffffffff
        if crc != crc32:
            raise CRCError("crc")
    key = block[24:24 + ksz]
    value = block[24 + ksz:24 + ksz + vsz]
    if decompress_value and (flag & FLAG_COMPRESS):
        value = quicklz.decompress(value)
        flag -= FLAG_COMPRESS
    return (key, vsz, value, flag, tstamp, ver)


def get_first_record_timestamp(data_path):
    with open(data_path, 'r') as f:
        while True:
            block = f.read(PADDING)
            crc, tstamp, flag, ver, ksz, vsz = struct.unpack("IiiiII", block[:REC_HEAD_SIZE])
            if not (0 < ksz <= MAX_KEY_LEN) or not 0 <= vsz <= MAX_VALUE_SIZE:
                print >>sys.stderr, 'record error in %s' % data_path
                continue
            # 为了节省时间，不在这里验证 crc 值了，因为 doubanfs 的值可能比较大，
            # 而且其备份是在 /backup 路径上，带宽较小。
            return tstamp


def filter_data_files(data_files, start_ts, stop_ts):
    if start_ts is None:
        start_ts = 0
    if stop_ts is None:
        stop_ts = float("inf")

    data_files = sorted(data_files, reverse=True)
    rs = []
    last_ts = int(time.time())
    for f in data_files:
        ts = get_first_record_timestamp(f)

        """
        range a = [start_ts, stop_ts]
        range b = [ts, last_ts]

        这个两个区间不重合只有两种情况：

        1. 由于我们遍历 data 文件是从文件号最大的开始的，所以此时，我应该跳过
        当前文件继续遍历。
                 a              b
        _____|______|______|_________|____

        2. 这时说明以后也不会有重合的集合了，break 退出。
                 b              a
        _____|______|______|_________|____
        """
        if last_ts < start_ts:
            break
        last_ts = ts
        if stop_ts < ts:
            continue
        rs.append(f)
    return rs


class DataFile(object):
    def __init__(self, path, check_crc=True, decompress_value=True,
                 stop_on_bad=True):
        self.path = path
        self.stop_on_bad = stop_on_bad
        self.check_crc = check_crc
        self.decompress_value = decompress_value

        self.num_bad = 0
        self.f = open(path, 'r')
        self.last_err = None

    def close(self):
        self.f.close()

    def seek(self, pos):
        return self.f.seek(pos & 0xffffff00, 0)

    def pos(self):
        return self.f.tell()

    def get_last_error(self):
        return self.last_err

    def __iter__(self):
        return self

    def next(self):
        try:
            pos = self.pos()
            rec = read_record(self.f, self.decompress_value, self.check_crc)
            if rec is None:
                raise StopIteration()
            return (pos, rec)
        except StopIteration as e:
            raise e
        except Exception as e:
            if self.stop_on_bad:
                raise e
            else:
                self.num_bad += 1
                self.last_err = e
                return (pos, None)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()


### tools

def get_first_record(datapath):
    with open(datapath, 'r') as f:
        return  read_record(f)


def get_rec_time_str(ts):
    return time.strftime("%Y%m%d-%H:%M:%S", time.localtime(ts))


def write_record(f, key, value, flag, ts, ver):
    header = struct.pack('IiiII', ts, flag, ver, len(key), len(value))
    crc32 = binascii.crc32(header)
    crc32 = binascii.crc32(key, crc32)
    crc32 = binascii.crc32(value, crc32) & 0xffffffff
    f.write(struct.pack("I", crc32))
    f.write(header)
    f.write(key)
    f.write(value)
    rsize = 24 + len(key) + len(value)
    if rsize & 0xff:
        f.write('\x00' * (PADDING - (rsize & 0xff)))
        rsize = ((rsize >> 8) + 1) << 8
    return rsize


def main():
    import signal
    import argparse
    from beansdbadmin.core.hash import get_vhash, get_khash64

    parser = argparse.ArgumentParser(description="dump beansdb data content",
                                     prefix_chars="-")
    parser.add_argument('--show-value', action='store_true',
                        help="restore values, ignore pickled ones")

    parser.add_argument('--no-vhash', action='store_true',
                        help="vhash showd as 0, save time for decompress")
    parser.add_argument('--no-header', action='store_true',
                        help="do not print header and tailer")
    parser.add_argument('--start-pos', default=0, type=int)
    parser.add_argument('--stop-pos', default=0, type=int)
    parser.add_argument('--stop-bad', action='store_true')
    parser.add_argument('datafile')
    args = parser.parse_args()

    i = 0
    header = "NO. pos(hex) rsize(hex) flag(hex) ver ts time_str ksz vsz vhash key rvalue"
    if not args.no_header:
        print(header)

    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    decompress_value = args.show_value or (not args.no_vhash)
    with DataFile(args.datafile, True, decompress_value, args.stop_bad) as f:
        f.seek(args.start_pos)
        i = 0
        for (pos, rec) in f:
            if rec is None:
                print i, hex(pos), f.get_last_error()
                continue
            (key, vsz, value, flag, tstamp, ver) = rec
            vhash = 0
            ksz = len(key)
            rsize = get_record_size(ksz, vsz)
            time_str = get_rec_time_str(tstamp)
            if not args.no_vhash:
                vhash = get_vhash(value)
                if not args.show_value:
                    value = ""
                else:
                    value = restore_value(flag, value)

            print "%d %x %x %x %d %d %s %d %d %d %016x [%s] [%s]" % \
                  (i, pos, rsize, flag, ver, tstamp,
                   time_str, ksz, vsz, vhash, get_khash64(key), key, value)

            if args.stop_pos > 0 and f.pos() >= args.stop_pos:
                break
            i += 1
        print "num_bad", f.num_bad

    if not args.no_header:
        print header


if __name__ == "__main__":
    main()
