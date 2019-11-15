import os
import sys
from sys import stdin
import redis
from acaisdk.meta import Meta


STR_PREFIX = '[ACAI_TAG]'
NUM_PREFIX = '[ACAI_TAG_NUM]'
job_meta = {}
fileset_meta = {}


def parse_line(line, str_prefix, num_prefix):
    global job_meta, fileset_meta
    try:
        if line.startswith(str_prefix) or line.startswith(num_prefix):
            prefix, entity, kv_pair = line.strip().split(maxsplit=3)
            k, v = kv_pair.split('=', 1)
            if prefix == num_prefix:
                v = float(v)

            if entity.lower() == 'job':
                job_meta[k] = v
            elif entity.lower() == 'fileset':
                fileset_meta[k] = v
    except Exception as e:
        print(e)


def commit(fileset, jobid):
    global job_meta, fileset_meta
    try:
        Meta.update_file_set_meta(fileset, [], fileset_meta)
        Meta.update_job_meta(jobid, [], job_meta)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.exit(1)

    job_id = sys.argv[1]
    user_id = sys.argv[2]
    redis_host = sys.argv[3]
    redis_port = sys.argv[4]
    redis_pwd = sys.argv[5] if len(sys.argv) == 6 else ""

    r = redis.Redis(host=redis_host, port=redis_port, password=redis_pwd)

    line = stdin.readline()
    while line:
        sys.stdout.write(line)
        parse_line(line, STR_PREFIX, NUM_PREFIX)
        r.publish("log", "{}:{}:{}".format(job_id, user_id, line))
        line = stdin.readline()

    commit(os.environ["OUTPUT_FILE_SET"], job_id)
