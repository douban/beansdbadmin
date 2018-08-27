#!/usr/bin/env python
# encoding: utf-8

import os.path
import yaml


def get_server_conf(conf_dir):
    with open(os.path.join(conf_dir, "global.yaml")) as f:
        gconf = yaml.load(f)

    localpath = os.path.join(conf_dir, "local.yaml")
    if os.path.exists(localpath):
        with open(localpath) as f:
            lconf = yaml.load(f)
        return update_dict(gconf, lconf)
    else:
        return gconf


def update_dict(old, up):
    new = dict()

    for k,v0 in old.items():
        v1 = up.get(k)
        if v1 is None:
            new[k] = v0
        elif isinstance(v0, dict) and isinstance(v1, dict):
            new[k] = update_dict(v0, v1)
        elif type(v0) != type(v1):
            raise ValueError
        else:
            new[k] = v1

    for k,v1 in up.items():
        v0 = old.get(k)
        if v0 is None:
            new[k] = v1

    return new

if __name__ == "__main__":
    old = {1:{2:[3]}, 6:0}
    up = {1:{2:[4]}, 5:0}
    print update_dict(old, up)

