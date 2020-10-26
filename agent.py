import os
import subprocess
import sys
import zipfile
from os import path
import time
from shutil import copy2
from distutils.dir_util import copy_tree

import redis as redis

from acaisdk.fileset import FileSet
from acaisdk.file import File
from acaisdk.meta import *


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


def download_input_and_code(project_id, input_file_set):
    fileset_id = Meta.get_file_set_meta(input_file_set)['data'][0]["_id"]
    fileset_hash = Meta.get_file_set_meta(input_file_set)['data'][0]["__hash__"]

    # code_file_id = Meta.get_file_meta(code)['data'][0]["_id"]
    # code_file_hash = Meta.get_file_set_meta(input_file_set)['data'][0]["__hash__"]

    match_file_set = Meta.find_file( \
        Condition("__cached__").value(True), \
        Condition("__hash__").value(fileset_hash), \
    )
    # match_code_file = Meta.find_file( \
    #     Condition("__cached__").value(True), \
    #     Condition("__hash__").value(code_file_hash), \
    # )

    project_cache_folder = os.path.join('cache', project_id)
    if not os.path.exists(project_cache_folder):
        os.makedirs(project_cache_folder)

    if match_file_set['status'] == 'success' & len(match_file_set['data']) > 0:
        print("Downloading input file set from cache")

        cached_file_id = match_file_set['data'][0]['_id']
        to_dir = os.path.dirname(os.path.realpath('__file__'))
        from_dir = os.path.join(project_cache_folder, cached_file_id)
        copy_tree(from_dir, to_dir)
    else:
        print("Downloading input file set from Data Lake")

        FileSet.download_file_set(input_file_set, ".", force=True)
        try:
            copy_tree('.', os.path.join(project_cache_folder, fileset_id))
        except:
            print('copy_tree failed', os.path.join(project_cache_folder, fileset_id))
        Meta.update_file_set_meta(input_file_set, [], {'__cached__' : True})
    
    # TODO: also check for code file
    code_path = "./" + code
    File.download({code: code_path})
    with zipfile.ZipFile(code_path, "r") as ref:
        ref.extractall()


if __name__ == "__main__":
    try:
        project_id = os.environ["PROJECT_ID"]
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
        use_cache = os.environ["USE_CACHE"]
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
        publisher.progress("Downloading")

        if use_cache == "true":
            print("use_cache is true, checking cache")
            download_input_and_code(project_id, input_file_set)
        else:
            print("use_cache is false, downloading from data lake")
            FileSet.download_file_set(input_file_set, ".", force=True)

            # Download and unzip code
            code_path = "./" + code
            File.download({code: code_path})
            with zipfile.ZipFile(code_path, "r") as ref:
                ref.extractall()

        # Run user code
        publisher.progress("Running")
        print("Running user code with command " + command)

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

        ret_code = p.poll()
        if ret_code != 0:
            publisher.progress("Failed")
            print("Error code: " + str(ret_code))
            sys.exit(ret_code)

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

            print("remote output path: " + remote_output_path)

            output_file_set = File.upload(
                l_r_mapping).as_new_file_set(output_file_set)["id"]

            # Update job meta data
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
