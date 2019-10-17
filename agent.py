import requests
import subprocess
import os, shutil, sys
import virtualenv
import zipfile
import json

REDIS_HOST_NAME = "10.128.62.19"


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
    __r = redis.Redis(host=REDIS_HOST_NAME, port=6379)

    def __init__(self, job_id, user_id):
        self.__job_id = job_id
        self.__user_id = user_id

    def progress(self, message):
        self.__r.publish(
            "progress", "%d:%d:%s".format(self.__job_id, self.__user_id, message)
        )

    def log(self, message):
        self.__r.publish(
            "log", "%d:%d:%s".format(self.__job_id, self.__user_id, message)
        )


if __name__ == "__main__":
    try:
        job_id = int(os.environ["JOB_ID"])
        user_id = int(os.environ["USER_ID"])
        input_file_set = os.environ["INPUT_FILE_SET"]
        output_path = os.environ["OUTPUT_PATH"]
        output_file_set = os.environ["OUTPUT_FILE_SET"]
        code = os.environ["CODE"]
        command = os.environ["COMMAND"]
        output_folder = os.environ["OUTPUT_FOLDER"]
        data_lake = os.environ["DATA_LAKE"]
    except (KeyError, NameError):
        sys.exit(1)

    publisher = Publisher(job_id, user_id)

    with cd("acai-job"):
        with cd("input"):
            publisher.progress("Downloading")
            File.downloadFileSet(input_file_set, ".") # TODO: implement this

        # Create output folder so user can dump output files in it.
        with cd(output_folder):
            pass

        with cd("code"):
            # Download and unzip code
            File.download({code: "code.zip"})
            with zipfile.ZipFile("code.zip", "r") as ref:
                ref.extractall()

            publisher.progress("Running")
            user_code = subprocess.Popen(
                command, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True
            )

            log_publisher = subprocess.Popen(
                ["python3", "../../log_publisher.py", str(job_id)],
                stdin=user_code.stdout,
                stdout=subprocess.PIPE,
            )

        user_code.stdout.close()

        publisher.progress("Uploading")

        with cd(output_folder):
            # TODO: Upload all files in this folder and create an output fileset

        with cd("log"):
            with open("stdout.txt", "w") as f:
                f.write(log_publisher.stdout.read().decode())
            with open("stderr.txt", "w") as f:
                f.write(user_code.stderr.read().decode())
            # TODO: Optionally upload these logs to data lake
        
        #TODO: Call metadata server to upload file metadata

        publisher.progress("Finished")
