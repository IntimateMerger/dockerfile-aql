#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import digdag
import logging
import subprocess
import uuid
import sys
import time
import aerospike
from aerospike import predicates

logger = logging.getLogger(__name__)


class Aerospike(object):

    def __init__(self):
        pass

    def query(self, as_host, ns_name, set_name, key_name, days, s3_bucket, s3_key):
        now = int(time.time())
        min_ts = now - (60 * 60 * 24 * days)
        max_ts = 2147483647  # 2038-01-19 12:14:07

        fn = '/tmp/{0}.txt'.format(str(uuid.uuid4()))

        cl = aerospike.client({'hosts': [(as_host, 3000)]}).connect()
        q = cl.query(ns_name, set_name)
        q.select(key_name)
        q.where(predicates.between("ts", min_ts, max_ts))

        def write_id((key, meta, bins)):
            f.write(bins[key_name] + "\n")

        logger.info('aql: SELECT {0} FROM {1}.{2} WHERE ts > {3}'.format(key_name, ns_name, set_name, min_ts))
        logger.info('Start Query ...')
        with open(fn, "w") as f:
            q.foreach(write_id, {"timeout": 0})
        logger.info('Finish Query ...')

        cl.close()
        s3_mv(fn, 's3://{0}/{1}'.format(s3_bucket, s3_key))


def s3_mv(fn, key):
    cmd = "aws s3 mv {0} {1}".format(fn, key)
    logger.info("Run cmd: " + cmd)
    subprocess.check_call(cmd, shell=True)


if __name__ == "__main__":
    if len(sys.argv) == 8:
        as_host = str(sys.argv[1])
        ns_name = str(sys.argv[2])
        set_name = str(sys.argv[3])
        key_name = str(sys.argv[4])
        days = int(sys.argv[5])
        s3_bucket = str(sys.argv[6])
        s3_key = str(sys.argv[7])

        a = Aerospike()
        a.query(as_host, ns_name, set_name, key_name, days, s3_bucket, s3_key)

    else:
        print("usage: python -u query.py [hostname] [namespace] [set] [key] [days]")
