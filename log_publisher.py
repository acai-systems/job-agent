import os
import sys
from sys import stdin

import redis

REDIS_HOST_NAME = "10.128.62.19"

if __name__ == "__main__":
    job_id = sys.argv[1]

    r = redis.Redis(host=REDIS_HOST_NAME, port=6379)

    line = stdin.readline()

    while line:
        sys.stdout.write(line)
        r.publish("log", "{}:{}".format(job_id, line))
        line = stdin.readline()
