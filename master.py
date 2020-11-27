import json
import numpy as np
import socket
import threading
import sys
import time


class task_handler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.robin_index = 0

    def random_schedular(self):
        while True:
            random_worker = np.random.randint(0, 2)
            if(Workers[workerIndex[random_worker]]['free_slots']):
                return workerIndex[random_worker]

    def round_robin(self):
        temp = 0
        while True:
            if(Workers[workerIndex[self.robin_index]]['free_slots']):
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
        # acquire lock
        new_task = TaskPool.pop(0)
        # release lock
        if(algo_type == 'RANDOM'):
            free_worker = self.random_schedular()
        elif(algo_type == 'RR'):
            free_worker = self.round_robin()
        elif(algo_type == 'LL'):
            free_worker = self.least_loaded()
        Workers[free_worker]['free_slots'] -= 1
        workerName = ''
        workerPort = Workers[free_worker]['port']
        assignSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        assignSocket.connect((workerName, workerPort))
        assignSocket.send(json.dumps(new_task).encode())
        modifiedMessage = assignSocket.recv(1048576).decode()
        assignSocket.close()


class job_request_handler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while 1:
            client, clientaddr = requestSocket.accept()
            message = client.recv(1048576)
            modifiedMessage = message.decode().upper()
            newJob = json.loads(modifiedMessage)
            Jobs[newJob["JOB_ID"]] = {"map_tasks": newJob["MAP_TASKS"], "map_task_remaining": len(
                newJob["MAP_TASKS"]), "reduce_tasks": newJob["REDUCE_TASKS"], "reduce_task_remaining": len(newJob["REDUCE_TASKS"])}
            for i in Jobs[newJob["JOB_ID"]]["map_tasks"]:
                elem = dict(i)
                elem["job_id"] = newJob["JOB_ID"]
                elem["type"] = "MAP"
                # acquire lock
                TaskPool.append(elem)
                # release lock


algo_type = sys.argv[2]
f = open(sys.argv[1], 'r')

workerConfig = json.load(f)
Workers = {i['worker_id']: {'max_slots': i['slots'], 'port': i['port'],
                            'free_slots': i['slots']} for i in workerConfig["workers"]}
workerIndex = sorted([i['worker_id'] for i in Workers])
Jobs = dict()
TaskPool = []


requestPort = 5000
requestSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
requestSocket.bind(('', requestPort))
requestSocket.listen(1)

job_handle = job_request_handler()
task_spawn = task_handler()
job_handle.start()
task_spawn.start()
job_handle.join()
task_spawn.join()

requestSocket.close()