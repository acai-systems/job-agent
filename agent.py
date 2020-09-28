import os
import subprocess
import sys
import zipfile
from os import path
import time

import redis as redis

from acaisdk.fileset import FileSet
from acaisdk.file import File
from acaisdk.meta import Meta


class cd:
    def __init__(self, newPath):
        self.newPath = newPath

    def __enter__(self):
        if not os.path.exists(self.newPath):
            os.mkdir(self.newPath)
        self.oldPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.oldPath)


class Publisher:
    def __init__(self, job_id, user_id, job_type, host, port, pwd=None):
        self.__job_id = job_id
        self.__user_id = user_id
        self.__job_type = job_type
        self.__r = redis.Redis(host=host, port=port, password=pwd)

    def progress(self, message):
        self.__r.publish(
            "job_progress",
            "{}:{}:{}:{}".format(
                self.__job_id, self.__user_id, self.__job_type, message
            ),
        )


STR_PREFIX = "[ACAI_TAG]"
NUM_PREFIX = "[ACAI_TAG_NUM]"
fileset_meta = {}


def parse_tag_requests(line):
    global job_meta, fileset_meta, STR_PREFIX, NUM_PREFIX
    try:
        if line.startswith(STR_PREFIX) or line.startswith(NUM_PREFIX):
            prefix, entity, kv_pair = line.strip().split(maxsplit=2)
            k, v = kv_pair.split("=", maxsplit=1)
            if prefix == NUM_PREFIX:
                v = float(v)

            if entity.lower() == "fileset":
                fileset_meta[k] = v
    except Exception as e:
        return "[ACAI_ERROR] {}".format(e)


if __name__ == "__main__":
    try:
        job_id = os.environ["JOB_ID"]
        user_id = os.environ["USER_ID"]
        job_type = os.environ["JOB_TYPE"]
        input_file_set = os.environ["INPUT_FILE_SET"]
        output_path = os.environ["OUTPUT_PATH"]
        output_file_set = os.environ["OUTPUT_FILE_SET"]
        code = os.environ["CODE"]
        command = os.environ["COMMAND"]
        data_lake = os.environ["DATA_LAKE_MOUNT_PATH"]
        redis_host = os.environ["REDIS_HOST"]
        redis_port = os.environ["REDIS_PORT"]
        redis_pwd = os.environ["REDIS_PWD"]
    except (KeyError, NameError) as e:
        print(e)
        sys.exit(1)

    publisher = Publisher(
        job_id,
        user_id,
        job_type,
        host=redis_host,
        port=redis_port,
        pwd=redis_pwd)

    with cd(data_lake):
        publisher.progress("Downloading input fileset " + input_file_set)
        FileSet.download_file_set(input_file_set, ".", force=True)

        # Download and unzip code
        code_path = "./" + code
        publisher.progress("Downloading code " + code_path)
        File.download({code: code_path})
        with zipfile.ZipFile(code_path, "r") as ref:
            ref.extractall()

        # Run user code
        publisher.progress("Running user code")

        start = time.time()
        p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            executable="/bin/bash",
        )

        while p.poll() is None:
            line = p.stdout.readline()
            sys.stdout.write(line.decode())
            sys.stdout.flush()
            parse_tag_requests(line.decode())

        for line in p.stdout:
            sys.stdout.write(line.decode())
            sys.stdout.flush()
            parse_tag_requests(line.decode())

        end = time.time()

        if p.poll() != 0:
            publisher.progress("Failed")
            sys.exit(1)

        # Upload output and create output file set. Skip for profiling jobs.
        if output_path:
            publisher.progress("Uploading")

            remote_output_path = (
                output_path[1:] if output_path[0] == "." else output_path
            )
            remote_output_path = path.join("/", remote_output_path)
            remote_output_path += "" if remote_output_path.endswith(
                "/") else "/"
            l_r_mapping, _ = File.convert_to_file_mapping(
                [output_path], remote_output_path
            )

            output_file_set = File.upload(
                l_r_mapping).as_new_file_set(output_file_set)["id"]

            try:
                Meta.update_file_set_meta(output_file_set, [], fileset_meta)
            except Exception as e:
                print(e)

        # Job finished, message format:
        # <job_id>:<user_id>:<job_type>:Finished:<runtime>:<finish_time>:<upload_fileset_name>
        publisher.progress(
            "Finished:{}:{}:{}".format(
                int(end - start), int(time.time()), output_file_set
            )
        )
