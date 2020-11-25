import json
import numpy as np 
import socket
import threading
import sys
import time

Jobs = dict()

class job_handler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.robin_index = 0

    def random_schedular(self):
        while True:
            random_worker = random.randint(0,2)
            if(Worker[workerIndex[random_worker]]['free_slots']):
                return workerIndex[random_worker]
        
    def round_robin(self):
        temp = 0
        while True:
            if(Worker[workerIndex[self.robin_index]]['free_slots']):
                temp = self.robin_index
                self.robin_index = (self.robin_index + 1) % len(workerIndex)
                return workerIndex[temp]

    def least_loaded(self):
        while True:
            max_index = max(Worker,lambda x: x["free_slots"])
            if(Worker[max_index]["free_slots"]):
                return max_index
            time.sleep(1)
            

    def run(self):
        while 1:
            client,clientaddr=requestSocket.accept()
            message = client.recv(2048)
            modifiedMessage = message.decode().upper()
            newJob = json.loads(modifiedMessage)
            Jobs[newJob["JOB_ID"]] = {"map_tasks" : newJob["MAP_TASKS"] , "reduce_tasks" : newJob["REDUCE_TASKS"]}     

            if(algo_type == 'RANDOM'):
                free_worker = self.random_schedular()     
            elif(algo_type == 'RR'):
                free_worker = self.round_robin()
            elif(algo_type == 'LL'):
                free_worker = self.least_loaded()

            client.close()
            

algo_type = sys.argv[2] 
f = open(sys.argv[1],'r')

workerConfig = json.load(f)
Workers = {i['worker_id']:{'max_slots':i['slots'],'port':i['port'],'free_slots':i['slots']} for i in workerConfig["workers"]}
workerIndex = sorted([i['worker_id'] for i in Workers])


requestPort = 5000
requestSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
requestSocket.bind(('', requestPort))
requestSocket.listen(1)

job_spawn = job_handler()
job_spawn.start()
job_spawn.join()

requestSocket.close()