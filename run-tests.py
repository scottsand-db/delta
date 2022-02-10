#!/usr/bin/env python

#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import subprocess
from os import path


def run_sbt_tests(root_dir, scala_version=None):
    print("##### Running SBT tests #####")
    sbt_path = path.join(root_dir, path.join("build", "sbt"))
    if scala_version is None:
        run_cmd([sbt_path, "clean", "+test"], stream_output=True)
    else:
        run_cmd([sbt_path, "clean", "++ %s test" % scala_version], stream_output=True)


def run_python_tests(root_dir):
    print("##### Running Python tests #####")
    python_test_script = path.join(root_dir, path.join("python", "run-tests.py"))
    print("Calling script %s", python_test_script)
    run_cmd(["python", python_test_script], stream_output=True)


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code is not 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return (exit_code, stdout, stderr)


if __name__ == "__main__":
    if os.getenv("USE_DOCKER") is not None:
        # prepare_docker_img = ["docker", "build", "--tag=pydeltalake", "."]
        # run_cmd(prepare_docker_img, stream_output=True)
        # # JENKINS_URL is passed here so that the Docker container
        # # can be in line with Jenkins build behavior(usage of sbt sources)
        # cmd = ["docker", "run", "-e", "JENKINS_URL",
        #        "-e", "SBT_1_5_5_MIRROR_JAR_URL", "pydeltalake:latest"]
        # run_cmd(cmd, stream_output=True)

        # The following code just runs SBT tests. Replace them with the above code when we figure
        # out how to get the docker image without hitting docker.com rate limit.
        root_dir = os.path.dirname(os.path.dirname(__file__))
        run_sbt_tests(root_dir)
    else:
        root_dir = os.path.dirname(os.path.dirname(__file__))
        scala_version = os.getenv("SCALA_VERSION")
        run_sbt_tests(root_dir, scala_version)
        # Python tests are skipped when using Scala 2.13 as PySpark doesn't support it.
        if scala_version is None or scala_version.startswith("2.12"):
            run_python_tests(root_dir)
