#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
import aerospike
from aerospike import predicates as p

if __name__ == "__main__":
    if len(sys.argv) == 6:
        as_host = str(sys.argv[1])
        ns_name = str(sys.argv[2])
        st_name = str(sys.argv[3])
        key_name = str(sys.argv[4])
        days = int(sys.argv[5])

        now = int(time.time())
        min_ts = now - (60 * 60 * 24 * days)
        max_ts = 2147483647  # 2038-01-19 12:14:07

        cl = aerospike.client({'hosts': [(as_host, 3000)]}).connect()
        query = cl.query(ns_name, st_name)
        query.select(key_name)
        query.where(p.between("ts", min_ts, max_ts))

        def echo((key, meta, bins)):
            print(bins[key_name])

        query.foreach(echo, {"timeout": 0})

    else:
        print("usage: python -u query.py [hostname] [namespace] [set] [key] [days]")
