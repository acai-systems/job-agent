import os
import subprocess
import sys
import zipfile
from os import path
import time
import timeit
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


def check_input_file_set(project_id, input_file_set):
    try:
        fileset_hash = Meta.get_file_set_meta(input_file_set)['data'][0]["__hash__"]

        match_file_set = Meta.find_file_set( \
            Condition("__cached__").value(True), \
            Condition("__hash__").value(fileset_hash))
        
        cache_project_folder = os.path.join(os.path.dirname(os.path.realpath('__file__')), project_id)

        cached_file_id = ""
        if match_file_set['status'] == 'success' and len(match_file_set['data']) > 0:
            cached_file_id = match_file_set['data'][0]['_id']
        
        if cached_file_id == "" or \
           not os.path.exists(os.path.join(cache_project_folder, cached_file_id)) or \
           not os.listdir(os.path.join(cache_project_folder, cached_file_id)):
           
           cached_file_id = Meta.get_file_set_meta(input_file_set)['data'][0]['_id']
           # start = timeit.default_timer()
           FileSet.download_file_set(input_file_set, os.path.join(cache_project_folder, cached_file_id), force=True)
           # stop = timeit.default_timer()
           Meta.update_file_set_meta(input_file_set, [], {'__cached__' : True})
           # print('[cache]: cache miss, downloading from data lake, total time: ', stop - start)  

        return os.path.join(cache_project_folder, cached_file_id)
        
    except Exception as e:
        print(e)
        return ""


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
        cache = os.environ["CEPH_CACHE_MOUNT_PATH"]
        redis_host = os.environ["REDIS_HOST"]
        redis_port = os.environ["REDIS_PORT"]
        redis_pwd = os.environ["REDIS_PWD"]
        # use_cache = os.environ["USE_CACHE"]
        use_cache = "false"
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

    workspace = os.path.dirname(os.path.realpath('__file__'))
    cache = os.path.join(workspace, cache)
    cached_file_set_path = ""

    with cd(cache):
        print("*" * 20)
        print('listing files in %s' % cache)
        for path, subdirs, files in os.walk(os.path.dirname(os.path.realpath('__file__'))):
            for name in files:
                print(os.path.join(path, name))
        print("*" * 20)
        
        if use_cache == "true":
            cached_file_set_path = check_input_file_set(project_id, input_file_set)

    with cd(data_lake):
        publisher.progress("Downloading")

        if cached_file_set_path != "":
            # start = timeit.default_timer()
            copy_tree(cached_file_set_path, '.')
            # stop = timeit.default_timer()
            # print('[cache]: cache hit, downloading from cache, total time: ', stop - start)  

        else:
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
            remote_output_path = os.path.join("/", remote_output_path)

            remote_output_path += "" if remote_output_path.endswith(
                "/") else "/"
            l_r_mapping, _ = File.convert_to_file_mapping(
                [output_path], remote_output_path
            )

            print("remote output path: " + remote_output_path)

            output_file_set = File.upload(
                l_r_mapping).as_new_file_set(output_file_set)["id"]

            # Copy to cache 
            # local_output_path = os.path.abspath(output_path)
            # cache_output_path = os.path.join(cache, project_id, output_file_set)
            # copy_tree(local_output_path, cache_output_path)
            # fileset_meta['__cached__'] = True

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
