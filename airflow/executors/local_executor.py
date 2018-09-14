# -*- coding: utf-8 -*-
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

import multiprocessing
import subprocess
import time

from builtins import range

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State

PARALLELISM = configuration.get('core', 'PARALLELISM')


class LocalWorker(multiprocessing.Process, LoggingMixin):
    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.daemon = True

    def run(self):
        while True:
            key, command = self.task_queue.get()
            if key is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break
            self.log.info("%s running %s", self.__class__.__name__, command)
            command = "exec bash -c '{0}'".format(command)
            try:
                # check_call command 失败 subprocess 抛出异常
                subprocess.check_call(command, shell=True)
                # 代替subprocess 命令, 减少cpu使用率
                # 使用etcd, 后台daemon 形式，会影响使用. grpc 和python 使用问题
                # from airflow import jobs
                # from airflow.models import DagBag
                # from airflow.models import  TaskInstance
                # tmp = command.split(" ")
                # dag_id = tmp[2]
                # task_id = tmp[3]
                # execute_time = datetime.strptime(tmp[4], "%Y-%m-%dT%H:%M:%S.%f")
                # file_path = tmp[-1]
                # dagbag = DagBag(file_path)
                # dag = dagbag.dags[dag_id]
                # task = dag.get_task(task_id=task_id)
                # ti = TaskInstance(task, execute_time)
                # ti.refresh_from_db()
                # jobs.LocalTaskJob(task_instance=ti).run()
                state = State.SUCCESS
            except subprocess.CalledProcessError as e:
                state = State.FAILED
                self.log.error("Failed to execute task %s.", str(e))
                # TODO: Why is this commented out?
                # raise e
            self.result_queue.put((key, state))
            self.task_queue.task_done()
            time.sleep(1)


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
    """

    def start(self):
        self.queue = multiprocessing.JoinableQueue()
        self.result_queue = multiprocessing.Queue()
        self.workers = [
            LocalWorker(self.queue, self.result_queue)
            for _ in range(self.parallelism)
        ]

        for w in self.workers:
            w.start()

    def execute_async(self, key, command, queue=None):
        self.queue.put((key, command))

    def sync(self):
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        # Sending poison pill to all worker
        for _ in self.workers:
            self.queue.put((None, None))

        # Wait for commands to finish
        self.queue.join()
        self.sync()
