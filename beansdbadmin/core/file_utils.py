#!/usr/bin/env python
# coding:utf-8
'''utils using multi package
find record by hint/data
compare hint and data
'''

import os
import glob
import re
from beansdbadmin.core.hash import get_khash, get_vhash
from beansdbadmin.core.path import (change_path_dbhome,
                                    get_all_files_index,
                                    check_zero_len,
                                    MAX_CHUNK_ID)
from beansdbadmin.core.hint import HintFile, get_keyinfo_from_hint
from beansdbadmin.core.data import DataFile, R_KEY


def eq_(a, b, msg=None):
    """
    Shorthand for assert a == b, "%r != %r" % (a, b)
    """
    if not a == b:
        raise AssertionError(msg or "%r != %r" % (a, b))


class HintError(Exception):
    pass


def locate_key_with_hint(db_homes, db_depth, key, ver_=None):
    """ assume disk0 already have link,
        if key exists and valid return True, if key not exist return False
    """
    if isinstance(db_homes, (list, tuple)):
        db_home = db_homes[0]
    else:
        db_home = db_homes
    key_hash = get_khash(key)
    if db_depth == 1:
        sector = (key_hash >> 28) & 0xf
        sector_path = "%x" % (sector)
        g = glob.glob(os.path.join(db_home, sector_path, "*.hint.s"))
    elif db_depth == 2:
        sector1 = (key_hash >> 28) & 0xf
        sector2 = (key_hash >> 24) & 0xf
        sector_path = "%x/%x" % (sector1, sector2)
        g = glob.glob(os.path.join(db_home, sector_path, "*.hint.s"))
    else:
        raise NotImplementedError()
    for hint_file in g:
        r = get_keyinfo_from_hint(hint_file, key)
        if r is not None:
            pos, ver, hash_ = r
            data_file = re.sub(r'(.+)\.hint.s', r'\1.data',
                               os.path.basename(hint_file))
            data_file = os.path.join(db_home, sector_path, data_file)
            print "file", data_file, "pos", pos, "ver", ver
            if ver_ is not None and ver != ver_:
                continue
            assert check_data_with_key(data_file, key, ver_=ver,
                                       hash_=hash_ if ver_ > 0 else None,
                                       pos=pos)
            return True
    return False


def locate_key_iterate(db_homes, db_depth, key, ver_=None):
    """ assume disk0 already have link,
    Returns
        True if key exists and valid
        False if key not exist
    """
    if isinstance(db_homes, (list, tuple)):
        db_home = db_homes[0]
    else:
        db_home = db_homes
    key_hash = get_khash(key)
    if db_depth == 1:
        sector = (key_hash >> 28) & 0xf
        sector_path = "%x" % (sector)
        g = glob.glob(os.path.join(db_home, sector_path, "*.data"))
    elif db_depth == 2:
        sector1 = (key_hash >> 28) & 0xf
        sector2 = (key_hash >> 24) & 0xf
        sector_path = "%x/%x" % (sector1, sector2)
        g = glob.glob(os.path.join(db_home, sector_path, "*.data"))
    else:
        raise NotImplementedError()
    for data_file in g:
        print data_file
        if check_data_with_key(data_file, key, ver_=ver_):
            return True
    return False


def check_data_with_key(file_path, key, ver_=None, hash_=None, pos=None):
    """ if pos is None, iterate data file to match key and ver_,
        otherwise seek to pos and check key and ver_ and hash_
    """
    with DataFile(file_path, True) as f:
        if pos is not None:
            f.seek(pos)
        for (_, rec) in f:
            (key2, _, value, _, _, ver) = rec

            if pos is not None:
                eq_(key, key2)
                if ver_ is not None and ver_ != ver:
                    raise ValueError('%s key %s expect ver %s != %s',
                                     file_path, key, ver_, ver)
            else:
                if key != key2:
                    continue
                if ver_ is not None and ver_ != ver:
                    continue
            _hash = get_vhash(value)
            if hash_ is not None and _hash != hash_:
                raise ValueError("%s key %s expect hash 0x%x != 0x%x" %
                                 (file_path, key, hash_, _hash))
            return True
    return False


def build_key_list_from_hint(file_path):
    key_list = list()
    hf = HintFile(file_path, None, check_khash=False)
    for key, rmeta, _ in hf:
        _, pos, ver, vhash = rmeta
        key_list.append((pos & 0xffffff00, key, ver, vhash))
    key_list.sort(cmp=lambda a, b: cmp(a[0], b[0]))
    return key_list


def check_data_with_hint(data_file, hint_file):
    hint_keys = build_key_list_from_hint(hint_file)

    j = 0
    pos = 0
    with DataFile(data_file, True) as f:
        for (pos, rec) in f:
            (key, _, value, _, _, ver) = rec

            hint_key = hint_keys[j]
            if pos < hint_key[0]:
                continue
            elif pos > hint_key[0]:
                raise Exception('%s pos %s > hint pos %s' %
                                (data_file, pos, hint_key[0]))

            eq_(hint_key[1], key, "diff key %s: %s %s" %
                                  (data_file, key, hint_key[1]))
            eq_(hint_key[2], ver, "diff ver %s: %s %s" %
                                  (data_file, key, hint_key[2]))

            _hash = get_vhash(value)
            eq_(hint_key[3], _hash, "diff hash %s: %s, 0x%x != 0x%x" %
                (data_file, key, _hash, hint_key[3]))
            j += 1

    if j < len(hint_keys):
        raise HintError("data is less than hint: %s" % (data_file))


def check_data_hint_integrity(db_homes, db_depth, bucket=None,
                              begin_number=None, fix=False):
    index, ok = get_all_files_index(db_homes, db_depth)
    index_list = index.items()
    index_list.sort(lambda a, b: cmp(a[0], b[0]))
    for bucket_, num_ext_dict in index_list:
        if bucket is not None:
            if bucket_[:len(bucket)] != bucket:
                print bucket_[:len(bucket)], bucket
                continue
        nums = [x[0] for x in num_ext_dict.keys()]
        max_num = max(nums)
        print "bucket", bucket_, "max_num", max_num
        for i in xrange(max_num + 1):
            if begin_number is not None and i < begin_number:
                continue
            print bucket, i
            try:
                data_file = num_ext_dict.get((i, 'data'))
                hint_file = num_ext_dict.get((i, 'hint.qlz'))  # TODO
                if data_file and hint_file:
                    print data_file, hint_file
                    check_data_with_hint(data_file[0], hint_file[0])
            except HintError, e:
                print "Error:", e
                print "removing", hint_file
                os.remove(hint_file)
                link_path = change_path_dbhome(
                    hint_file, db_homes[0], db_depth)
                if os.path.islink(link_path):
                    print "unlink", link_path
                    os.unlink(link_path)


def check_bucket_data(bucket_files, bucket, readonly=True):
    ok = True
    holes = []
    last_no = -1
    dup_files = []
    for i in range(MAX_CHUNK_ID):
        if (i, 'data') in bucket_files:
            if last_no == i - 1:
                pass
            else:
                holes.append((last_no + 1, i))
            last_no = i
            data_files = bucket_files[(i, 'data')]
            if len(data_files) > 1:
                data_files = filter(
                    lambda x: check_zero_len(x, readonly=readonly),
                    data_files)
            hint_files = bucket_files[(i, 'hint.qlz')]
            if len(hint_files) > 1:
                hint_files = filter(
                    lambda x: check_zero_len(x, readonly=readonly),
                    hint_files)
            hint_dict = dict()
            for data_file in data_files:
                try:
                    key = check_data_first_key(data_file, bucket)
                    if key is None:
                        if len(hint_files) == 1 and len(data_files) == 1:
                            hint_dict[data_file] = hint_files[0]
                            hint_files = []
                            break
                    _hint = filter(
                        lambda hint: get_keyinfo_from_hint(hint, key) is not None,
                        hint_files)
                    if _hint:
                        _hint = _hint[0]
                        hint_files.remove(_hint)
                    else:
                        _hint = None
                    hint_dict[data_file] = _hint
                except ValueError, e:
                    print "NOTE:", e

            if len(hint_dict) > 1:
                dup_files.append(hint_dict)
            if hint_files:
                print "unmatched hint %s" % (hint_files)
                if not readonly:
                    for h in hint_files:
                        print "removed", h
                        os.remove(h)
                ok = False
    if holes:
        print "* holes:"
        for i in holes:
            print "bucket %s, no. %s" % (bucket, i)
    if dup_files:
        ok = False
        print "* duplicate files:"
        for x in dup_files:
            print "--------"
            for data, hint in x.items():
                s = os.stat(data).st_size
                _size = s / 1024 / 1024
                print "%s (%0.1fM)-> %s" % (data, _size, hint)
        print
    return ok


def check_data_first_key(file_path, bucket):
    """ if ok , return first_key """
    assert isinstance(bucket, tuple)
    assert isinstance(bucket[0], int)
    with DataFile(file_path, check_crc=False, decompress_value=False) as f:
        for (_, rec) in f:
            key = rec[R_KEY]
            _hash = get_khash(key)
            if len(bucket) == 1:
                if _hash >> 28 == bucket[0]:
                    return key
                raise ValueError("%s belongs to %x" % (file_path, _hash >> 28))
            elif len(bucket) == 2:
                if (_hash >> 28 == bucket[0] and
                        ((_hash >> 24) & 0xf) == bucket[1]):
                    return key
                raise ValueError("%s belongs to %x" % (file_path, _hash >> 24))
            else:
                raise NotImplementedError()
