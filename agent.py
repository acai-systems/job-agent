import os
import subprocess
import sys
import zipfile
from os import path

import redis as redis

from acaisdk.fileset import FileSet
from acaisdk.file import File


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
    def __init__(self, job_id, user_id, host, port, pwd=None):
        self.__job_id = job_id
        self.__user_id = user_id
        self.__r = redis.Redis(host=host, port=port, password=pwd)

    def progress(self, message):
        self.__r.publish(
            "job_progress", "{}:{}:{}".format(self.__job_id, self.__user_id, message)
        )


if __name__ == "__main__":
    try:
        job_id = os.environ["JOB_ID"]
        user_id = os.environ["USER_ID"]
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
        job_id, user_id, host=redis_host, port=redis_port, pwd=redis_pwd
    )

    with cd(data_lake):
        publisher.progress("Downloading")
        FileSet.download_file_set(input_file_set, ".", force=True)

        # Download and unzip code
        code_path = "./" + code
        File.download({code: code_path})
        with zipfile.ZipFile(code_path, "r") as ref:
            ref.extractall()

        # Run user code
        publisher.progress("Running")

        log_publisher = subprocess.Popen(
            [
                "python",
                "../job-agent/log_publisher.py",
                job_id,
                user_id,
                redis_host,
                redis_port,
                redis_pwd,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

        user_code = subprocess.call(
            command, shell=True, executable="/bin/bash",
            stdout=log_publisher.stdin, stderr=log_publisher.stdin
        )

        log_publisher.stdin.close()

        # TODO: THIS IS TEMPORARY. Should send log file to log server for persistence
        if not path.exists(output_path):
            os.makedirs(output_path)
        log_file = path.join(output_path, "job_{}_log.txt".format(job_id))
        with open(log_file, "w") as f:
            f.write(log_publisher.stdout.read().decode())

        remote_output_path = output_path[1:] if output_path[0] == "." else output_path
        remote_output_path = path.join("/", remote_output_path) + "/"
        l_r_mapping, _ = File.convert_to_file_mapping([output_path], remote_output_path)

        if user_code != 0:
            publisher.progress("Failed")
            # TODO: THIS IS TEMPORARY
            File.upload(l_r_mapping)  # DO NOT create fileset
            sys.exit(0)

        # Upload output and create output file set
        print("publish")
        sys.stdout.flush()
        publisher.progress("Uploading")
        print("upload yoyoyo")
        sys.stdout.flush()
        uploaded = File.upload(l_r_mapping).as_new_file_set(output_file_set)
        print("upload done")
        sys.stdout.flush()

        # Job finished
        publisher.progress("Finished:{}".format((uploaded["id"])))
