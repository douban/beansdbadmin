#!/usr/bin/env python
# encoding: utf-8
'''anything about path, file_name, disk, size'''

import os
import re
import glob
import subprocess
import collections
import time

# utils
MAX_CHUNK_ID = 999


def is_empty_dir(dir_):
    if dir_ == '/':
        return False
    for root, _, names in os.walk(dir_, followlinks=True):
        for name in names:
            filepath = os.path.join(root, name)
            if os.path.isfile(filepath):
                s = os.stat(filepath)
                if s.st_size > 0:
                    return False
    return True


def get_disk_free(path, is_disk=False):
    if not is_disk:
        path = get_disk(path)
    s = os.statvfs(os.path.realpath(path))
    return path, s.f_bfree * s.f_bsize


def du(dir_path):
    cmd = ['du', '-s', dir_path]
    for loop in range(3):
        try:
            output = subprocess.check_output(cmd)
            break
        except subprocess.CalledProcessError:
            if loop < 2:
                time.sleep(0.3)
                continue
            else:
                raise
    return int(output.split()[0])


# simple
def make_sure_no_data(dir_):
    if not is_empty_dir(dir_):
        raise Exception('unexpected: has data in %s' % (dir_))


def home_to_homes(db_homes):
    if not isinstance(db_homes, (list, tuple)):
        db_homes = [db_homes]
    return db_homes


def homes_to_home(db_home):
    if not isinstance(db_home, (list, tuple)):
        db_home = db_home[0]
    return db_home


def get_max_fid(data_files):
    return max([int(os.path.basename(i)[:3]) for i in data_files])


def _get_all_files_glob(home, depth):
    sub_path = '/'.join(['*'] * (depth + 1))
    path = os.path.join(home, sub_path)
    return glob.glob(path)


def _get_all_files_walk(home):
    files = []
    for root, _, names in os.walk(home, followlinks=True):
        files.extend([os.path.join(root, name) for name in names])
    return files


def get_files_with_suffix(homes, suffixes=None, depth=None):
    homes = home_to_homes(homes)

    if depth is not None:
        assert 0 <= depth <= 2
    files = []
    for root in homes:
        if depth is not None:
            files.extend(_get_all_files_glob(root, depth))
        else:
            files.extend(_get_all_files_walk(root))

    if suffixes is None or len(suffixes) == 0:
        return files

    results = []
    for p in files:
        if any([p.endswith(suffix) for suffix in suffixes]):
            results.append(p)
    return results


def total_data_of_files(files):
    return sum([os.stat(f).st_size for f in files])


# parse a path

bucket_name_pattern = r'^[\da-f]$'
bucket_name_regx = re.compile(bucket_name_pattern)
fid_patten = r'[0-9][0-9][0-9]'
fid_regx = re.compile(fid_patten)
SUFFIXES = ['data', 'hint.qlz', 'htree']


def parse_bucket(name):
    if bucket_name_regx.match(name) is not None:
        return name


def parse_fid(fid_str):
    if fid_regx.match(fid_str) is not None and 0 <= int(fid_str) < MAX_CHUNK_ID:
        return int(fid_str)


def parse_basename(basename):
    '''return (int fid, string type_)

       fid is None if type_ is not valid, so only need to
       chec
       k with 'if type'  '''
    fid = None

    parts = basename.split('.', 1)
    if len(parts) != 2:
        return None, None
    fid_str, suffix = parts
    type_ = suffix if suffix in SUFFIXES else None
    if type_:
        fid = parse_fid(fid_str)
    return fid, type_


def make_basename(fid, suffix):
    if 0 <= fid < MAX_CHUNK_ID:
        return '%03d.%s' % (fid, suffix)


def make_path(db_home, buckets, basename):
    return os.path.join(db_home, "/".join(buckets), basename)


def parse_path(path, real=True, depth=None):
    ''' always return (home, int bucket tuple, fid, type)
    e.g.
    input /data1/xxx/doubandb/1/2/002.data
    output ('/data1/xxx/doubandb', ('1', '2'), 2, 'data')

    input /data1/xxx/1/2
    output (/data1/xxx/, ('1', '2'), None, None)

    input /data1/xxx/
    output (/data1/xxx/, (), None, None)
    '''
    path = os.path.abspath(path)
    if real:
        path = os.path.realpath(path)

    basename = os.path.basename(path)
    fid, type_ = parse_basename(basename)
    dir_ = os.path.dirname(path) if type_ else path
    dir_.rstrip('/')
    buckets = []
    while True:
        if depth is not None:
            if depth <= 0:
                break
            depth -= 1
        name = os.path.basename(dir_)
        bucket = parse_bucket(name)
        if bucket is None:
            break
        buckets.insert(0, bucket)
        dir_ = os.path.dirname(dir_)
    return (dir_, tuple(buckets), fid, type_)


def get_disk(path, real=True):
    '''return /data1 for  /data1/xxx/yyy/doubandb/1/2/zzz.data'''
    p = path
    if real:
        p = os.path.realpath(p)  # dir or file may not exist!

    if not os.path.exists(p):
        raise Exception("%s not exist" % (path))
    p = p.strip(os.path.sep)
    return '/' + os.path.split(p)[0]


def change_path_dbhome(file_path, new_home, db_depth):
    _, buckets, fid, suffix = parse_path(file_path, depth=db_depth)
    if suffix:
        return os.path.join(new_home, "/".join(buckets),
                            make_basename(fid, suffix))

# get info


def glob_by_type2(db_homes, db_depth, suffix):
    assert 0 <= db_depth <= 2
    sub_path = '*/' * db_depth + '[0-9][0-9][0-9].' + suffix
    paths = [os.path.join(db_home, sub_path) for db_home in db_homes]
    return reduce(lambda x, y: x + y, [glob.glob(path) for path in paths])


def scan_data_dirs(db_home, db_depth, nolink=True, bucket=None):
    ''' Check if there are too many (more than max_files) data files in
    single data directory.
    '''
    bucket_pattern = '[0-9a-fA-F]'
    if bucket:
        assert isinstance(bucket, int)
        bucket_pattern = "%x" % (bucket)
    if db_depth == 1:
        g = glob.glob('%s/%s/*.*' % (db_home, bucket_pattern))
    elif db_depth == 2:
        g = glob.glob('%s/%s/[0-9a-fA-F]/*.*' % (db_home, bucket_pattern))
    else:
        raise NotImplementedError()

    for file_ in g:
        if os.path.islink(file_):
            if nolink:
                continue
            yield os.path.realpath(file_)
        else:
            yield file_


def total_data_size(db_homes, db_depth, bucket=None):
    total = 0
    for db_home in db_homes:
        for file_ in scan_data_dirs(db_home, db_depth,
                                    nolink=True, bucket=bucket):
            s = os.stat(file_)
            total += s.st_size
    return total


def select_db_home_space_max(db_homes, file_size, reserve_space=None):
    if len(db_homes) == 1:
        return db_homes[0]
    db_homes_ = []
    for db_home in db_homes:
        space = get_space_of_dest(db_home)
        if reserve_space is not None and space < reserve_space:
            print space, reserve_space
            continue
        db_homes_.append((db_home, space))
    db_homes_.sort(lambda a, b: cmp(b[1], a[1]))
    if not db_homes_:
        raise Exception("no disk has the reserve_space %s" % (reserve_space))
    if db_homes_[0][1] > file_size:
        return db_homes_[0][0]


def get_data_files(db_homes, db_depth, buckets=range(16)):
    if isinstance(buckets[0], basestring):
        buckets = [int(x, 16) for x in buckets]
    for db_path in db_homes:
        for i in buckets:
            bucket_path = os.path.join(db_path, "%x" % (i))
            if db_depth == 1:
                yield bucket_path, glob.glob(os.path.join(bucket_path,
                                                          "*.data")), i
            elif db_depth == 2:
                for j in range(16):
                    sub_bucket_path = os.path.join(bucket_path, "%x" % (j))
                    yield sub_bucket_path, glob.glob(
                        os.path.join(sub_bucket_path, "*.data")), (i, j)


def get_disk_info(db_home, db_depth):
    disks_free = {}
    writing_disks = set()
    buckets = set()
    max_fid = 0
    bc_sizes = {}
    disk2buckets = collections.defaultdict(set)
    for _dir, data_files, bucket in get_data_files(db_home, db_depth):
        if data_files:
            max_fid = max(max_fid, get_max_fid(data_files))
            bc_sizes[bucket] = sum([os.path.getsize(f) for f in data_files])
            first_level_bucket = bucket if db_depth == 1 else bucket[0]
            buckets.add(first_level_bucket)
            disk = get_disk(_dir)
            writing_disks.add(disk)

            disks_free[disk] = 0
            disk2buckets[disk].add(first_level_bucket)
            for data_file in data_files:
                if os.path.islink(data_file):
                    disk = get_disk(data_file)
                    disks_free[disk] = 0
                    disk2buckets[disk].add(first_level_bucket)
    for disk in disks_free:
        disks_free[disk] = get_disk_free(disk, True)[1]

    return (list(buckets), list(writing_disks), max_fid, bc_sizes,
            disks_free, disk2buckets)


def get_all_files_index(db_homes, db_depth, readonly=True):
    db_homes = home_to_homes(db_homes)
    file_index = dict()
    ok = True
    for db_home in db_homes:
        for root, _, names in os.walk(db_home, followlinks=True):
            for file_name in names:
                file_path = os.path.join(root, file_name)
                if os.path.islink(file_path):
                    if not os.path.exists(file_path):
                        if not readonly:
                            print "bad link, removed", file_path
                            os.remove(file_path)
                        else:
                            print "bad link", file_path
                        ok = False
                        continue
                    target = os.readlink(file_path)
                    if os.path.islink(target):
                        print "double link %s -> %s" % (file_path, target)
                        if not readonly:
                            print "removed", file_path
                            os.remove(file_path)
                            print "removed", target
                            os.remove(target)
                        ok = False
                    continue
                elif (file_path.endswith('.hint.qlz') or
                      file_path.endswith('.data')):
                    ext = file_path[file_path.index('.') + 1:]
                    _, bucket, fid, _ = parse_path(file_path, True, db_depth)
                    bucket = tuple([int(x, 16) for x in bucket])
                    if not bucket in file_index:
                        file_index[bucket] = collections.defaultdict(list)
                    file_index[bucket][(fid, ext)].append(file_path)
    return file_index, ok


def get_space_of_dest(file_path):
    '''多处使用，通常参数是某个db_home, TODO: 用get_disk_free代替更好'''
    parent = None
    if not os.path.isdir(file_path):
        parent = file_path
        while True:
            parent = os.path.dirname(parent)
            parent = parent.rstrip("/")
            if not parent:
                raise Exception(
                    "cannot find a existing parent dir for %s" % (file_path))
            if os.path.exists(parent):
                break
            if os.path.islink(parent):  # not exists
                raise Exception("%s is a broken link" % (parent))
    else:
        parent = file_path
    s = os.statvfs(os.path.realpath(parent))
    return s.f_bfree * s.f_bsize


def _cal_bucket_max(dir_list):
    bucket_dict = collections.defaultdict(lambda: 0)
    for _, file_list in dir_list.iteritems():
        for file_path, size, bucket, number in file_list:
            if bucket_dict[bucket] < number:
                bucket_dict[bucket] = number
    return bucket_dict


def get_dirs_list(db_homes, db_depth, data_only=True, follow_link=False):
    """ return db_home ->  [ (file_path, size), ] """
    size_dict = collections.defaultdict(list)
    for db_home in db_homes:
        for root, _, names in os.walk(db_home):
            for name in names:
                if data_only:
                    if not name.endswith('.data'):
                        continue
                else:
                    if name.endswith('.htree'):
                        continue
                    elif (not name.endswith('.data') and
                          not name.endswith('.hint.qlz')):
                        continue
                file_path = os.path.join(root, name)
                if not follow_link and os.path.islink(file_path):
                    continue
                file_size = os.stat(file_path).st_size
                _, bucket, fid, _, = parse_path(file_path)
                size_dict[db_home].append(
                    (file_path, file_size, bucket, fid))
    return size_dict


def get_bucket_max(db_home_1st, db_depth):
    size_dict = get_dirs_list([db_home_1st], db_depth, follow_link=True)
    return _cal_bucket_max(size_dict)


def get_total_size(dir_list):
    """ returns [ (db_home, total_size) ]  and sorted by size """
    total_sizes = list()
    for db_home, file_list in dir_list.items():
        total_size = 0
        for v in file_list:
            size = v[1]
            total_size += size
        total_sizes.append((db_home, total_size))
    total_sizes.sort(cmp=lambda a, b: cmp(a[1], b[1]))
    return total_sizes

# checks


def check_zero_len(filepath, readonly=True):
    """ return True for ok, return False for not ok """
    assert os.path.exists(filepath)
    if os.stat(filepath).st_size > 0:
        return True
    if not readonly:
        print "%s zero size, removed" % (filepath)
        os.remove(filepath)
    else:
        print "%s zero size" % (filepath)
    return False


def delete_hint_and_htree(db_homes):
    files = get_files_with_suffix(
        db_homes, ['.hint.qlz', '.hint.s', '.htree', '.tree'])
    [os. remove(f) for f in files]


def delete_htree(db_homes):
    files = get_files_with_suffix(db_homes, ['.htree', '.tree'])
    [os. remove(f) for f in files]


def check_bc_files(path, newdata=False, curr=None):
    types = ["data", "hint.s", "hint.m", "collision", "tree"]
    files = [[0, 0, 0, 0] for i in range(MAX_CHUNK_ID)]
    max_id = -1
    tree_id = -1

    def assertEqual(i, lst):
        if files[i] != lst:
            raise Exception("path = %s, fid = %d, %s != %s" %
                            (path, i, files[i], lst))

    for _, _, names in os.walk(path):
        for name in names:
            if name == "buckets.txt":
                continue
            fid = int(name[:3])
            typ = types.index(name[4:])
            files[fid][typ] = 1
            if typ == 0 and fid > max_id:
                max_id = fid
            if typ != 4:
                files[fid][typ] = 1
            else:
                if tree_id == -1:
                    tree_id = fid
                else:
                    raise Exception(
                        "path = %s, more than one htree: %d and %d" %
                        (path, fid, tree_id))
    if curr is not None:
        assert max_id == curr

    max_merged = max_id
    if newdata:
        max_merged -= 1

    for i in range(MAX_CHUNK_ID):
        if i < max_merged:
            assertEqual(i, [1, 1, 0, 0])
        elif i == max_merged:
            assertEqual(i, [1, 1, 0 if i == 0 else 1, 1])
        elif i == max_id:
            assertEqual(i, [1, 0, 0, 0])
        else:
            assertEqual(i, [0, 0, 0, 0])


def check_hs_files(path, depth):
    if depth == 0:
        check_bc_files(path)
    else:
        for i in range(16):
            check_hs_files("%s/%x" % (path, i), depth - 1)


def _remove_current_file(dir_list, bucket_dict):
    for _bucket, max_no in bucket_dict.iteritems():
        for _, file_list in dir_list.iteritems():
            for file_path, size, bucket, number in file_list:
                if _bucket == bucket and number == max_no:
                    print "skip", _bucket, 'max file', number
                    file_list.remove((file_path, size, bucket, number, ))
    return dir_list


def check_exists_data(db_homes, bucket_str, db_depth):
    return any([glob.glob(os.path.join(path, bucket_str, "*.data" if db_depth == 1 else "*/*.data"))
                for path in db_homes])


def check_exists_file(db_homes, bucket, src_file):
    """ if there's a regular file in db_home contain the file,
        return the file path """
    assert isinstance(bucket, (list, tuple))
    assert isinstance(db_homes, (list, tuple))
    if isinstance(bucket[0], int):
        bucket_ = ["%x" % (x) for x in bucket]
    else:
        bucket_ = bucket
    for i, db_home in enumerate(db_homes):
        _path = os.path.join(db_home, *bucket_)
        _path = os.path.join(_path, os.path.basename(src_file))
        if os.path.isfile(_path) and not os.path.islink(_path):
            return _path, i
    return None, None


# move


def is_same_file(src, dst, with_time=True):
    '''return True iff same size and mtime'''
    _src = os.path.realpath(src)
    _dst = os.path.realpath(dst)
    if not os.path.exists(_src) or not os.path.exists(_dst):
        return False
    _size_size = os.stat(src).st_size
    _dst_size = os.stat(dst).st_size
    if _size_size != _dst_size:
        return False
    if with_time:
        if abs(os.path.getmtime(src) - os.path.getmtime(dst)) > 1:
            return False
    return True


if __name__ == '__main__':
    print get_disk_info(["/var/lib/beansdb"], 2)
