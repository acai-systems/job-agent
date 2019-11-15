import sys
from sys import stdin
import redis

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
        r.publish("log", "{}:{}:{}".format(job_id, user_id, line))
        line = stdin.readline()
