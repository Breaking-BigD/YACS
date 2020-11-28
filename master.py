import json
import numpy as np
import socket
import threading
import sys
import time


def mergeDict(dict1, dict2):
    d = dict()
    d.update(dict1)
    d.update(dict2)
    return d


class task_handler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.robin_index = 0

    def random_schedular(self):
        while True:
            random_worker = np.random.randint(0, 2)
            if Workers[workerIndex[random_worker]]['free_slots']:
                return workerIndex[random_worker]

    def round_robin(self):
        temp = 0
        while True:
            if Workers[workerIndex[self.robin_index]]['free_slots']:
                temp = self.robin_index
                self.robin_index = (self.robin_index + 1) % len(workerIndex)
                return workerIndex[temp]

    def least_loaded(self):
        while True:
            max_index = max(Workers, lambda x: Workers[x]["free_slots"])
            if(Workers[max_index]["free_slots"]):
                return max_index
            time.sleep(1)

    def run(self):
        while(1):
            if len(TaskPool):
                # acquire lock
                new_task = TaskPool.pop(0)
                # release lock
                if algo_type == 'RANDOM':
                    free_worker = self.random_schedular()
                elif algo_type == 'RR':
                    free_worker = self.round_robin()
                elif algo_type == 'LL':
                    free_worker = self.least_loaded()
                Workers[free_worker]['free_slots'] -= 1
                Jobs[new_task["job_id"]][new_task["type"] +
                                         "_tasks"][new_task["task_id"]]["status"] = 0
                Jobs[new_task["job_id"]][new_task["type"] +
                                         "_tasks"][new_task["task_id"]]["worker"] = free_worker
                workerName = ''
                workerPort = Workers[free_worker]['port']
                assignSocket = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                assignSocket.connect((workerName, workerPort))
                print(new_task)
                assignSocket.send(json.dumps(new_task).encode())
                modifiedMessage = assignSocket.recv(1048576).decode()
                assignSocket.close()


class job_request_handler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def job_object_maker(self, s):
        newJob = json.loads(s)
        newJob["remaining_map_tasks"] = len(newJob["map_tasks"])
        newJob["remaining_reduce_tasks"] = len(newJob["reduce_tasks"])
        newJob["map_tasks"] = {i["task_id"]: mergeDict(
            i, {"worker": None, "status": -1, "type": "map", "job_id": newJob["job_id"]}) for i in newJob["map_tasks"]}
        newJob["reduce_tasks"] = {i["task_id"]: mergeDict(
            i, {"worker": None, "status": -1, "type": "reduce", "job_id": newJob["job_id"]}) for i in newJob["reduce_tasks"]}
        print("newjob", newJob)
        return newJob

    def run(self):
        while 1:
            client, clientaddr = requestSocket.accept()
            message = client.recv(1048576)
            newJob = self.job_object_maker(message.decode())
            job_id = newJob["job_id"]
            # acquire lock
            Jobs[job_id] = newJob
            # release lock
            for elem in Jobs[job_id]["map_tasks"]:
                # acquire lock
                TaskPool.append(Jobs[job_id]["map_tasks"][elem])
                # release lock
            client.close()


class worker_response_handler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while 1:
            worker, workeraddr = responseSocket.accept()
            message = worker.recv(1048576)
            completedTask = json.loads(message.decode())
            for task in completedTask:
                print("completed task", task)
                # acquire lock
                Workers[Jobs[task["job_id"]][task["type"] + "_tasks"]
                        [task["task_id"]]["worker"]]["free_slots"] += 1
                # release lock
                Jobs[task["job_id"]][task["type"] +
                                     "_tasks"][task["task_id"]]["status"] = 1
                Jobs[task["job_id"]]["remaining_"+task["type"] + "_tasks"] -= 1
                if(not Jobs[task["job_id"]]["remaining_map_tasks"] and not Jobs[task["job_id"]]["remaining_reduce_tasks"]):
                    # acquire lock
                    print('\n',"completed job",Jobs.pop(task["job_id"]),'\n')
                    # release lock
                elif(task["type"] == "map" and not Jobs[task["job_id"]]["remaining_map_tasks"]):
                    for elem in Jobs[task["job_id"]]["reduce_tasks"]:
                        # acquire lock
                        TaskPool.append(Jobs[task["job_id"]]["reduce_tasks"][elem])
                        # release lock
            worker.close()


algo_type = sys.argv[2]
f = open(sys.argv[1], 'r')

workerConfig = json.load(f)
Workers = {i['worker_id']: {'max_slots': i['slots'], 'port': i['port'],
                            'free_slots': i['slots']} for i in workerConfig["workers"]}
workerIndex = sorted([i for i in Workers])
Jobs = dict()
TaskPool = []


requestPort = 5000
requestSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
requestSocket.bind(('', requestPort))
requestSocket.listen(1)

responsePort = 5001
responseSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
responseSocket.bind(('', responsePort))
responseSocket.listen(1)

job_handle = job_request_handler()
task_spawn = task_handler()
response_handle=worker_response_handler()
job_handle.start()
task_spawn.start()
response_handle.start()
job_handle.join()
task_spawn.join()
response_handle.join()

requestSocket.close()
