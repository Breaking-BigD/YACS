import json
import numpy as np 
import socket
import threading
import sys
import time

class task_executer(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while 1:
            if len(execution_slots):
                message_list=[]
                for curr_slot in execution_slots:
                    # acquire lock
                    execution_slots[curr_slot]["duration"] -= 1
                    if not execution_slots[curr_slot]["duration"]:
                        message_list.append(curr_slot)
                    # release lock
                time.sleep(1)
                if(len(message_list)):
                    pop_list = pop_list[::-1]
                    message = json.dumps(message_list)
                    masterName = ''
                    masterPort = 5001
                    updateSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    updateSocket.connect((masterName, masterPort))
                    updateSocket.send(message.encode())
                    modifiedMessage = updateSocket.recv(1048576).decode()
                    updateSocket.close()

class job_handler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while 1:
            master,masteraddr=requestSocket.accept()
            message = master.recv(1048576)
            modifiedMessage = message.decode().upper()
            newTask = json.loads(modifiedMessage)
            # acquire lock
            for curr_slot in execution_slots:
                execution_slots[curr_slot]=newTask 
            # release lock           
            master.close()
            
total_slots = int(sys.argv[2])
execution_slots={i:None for i in range(total_slots)}
requestPort = int(sys.argv[1])
requestSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
requestSocket.bind(('', requestPort))
requestSocket.listen(1)