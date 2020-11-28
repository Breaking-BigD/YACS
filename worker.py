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
            message_list = []
            for curr_slot in range(len(execution_slots)):
                # acquire lock
                if(execution_slots[curr_slot]):
                    execution_slots[curr_slot]["duration"] -= 1
                    if not execution_slots[curr_slot]["duration"]:
                        message_list.append(execution_slots[curr_slot])
                        execution_slots[curr_slot]=None
                # release lock
            time.sleep(1)
            if(len(message_list)):
                print(message_list)
                message = json.dumps(message_list)
                masterName = ''
                masterPort = 5001
                updateSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                updateSocket.connect((masterName, masterPort))
                updateSocket.send(message.encode())
                updateSocket.close()


class slot_handler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while 1:
            master, masteraddr = requestSocket.accept()
            message = master.recv(1048576)
            modifiedMessage = message.decode()
            newtask = json.loads(modifiedMessage)
            # acquire lock
            c = 0
            while(execution_slots[c] != None):
                c += 1
            execution_slots[c] = newtask
            # release lock
            master.close()


total_slots = int(sys.argv[2])
execution_slots = [ None for i in range(total_slots)]
requestPort = int(sys.argv[1])
requestSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
requestSocket.bind(('', requestPort))
requestSocket.listen(1)

slot_handle = slot_handler()
task_spawn = task_executer()
slot_handle.start()
task_spawn.start()
slot_handle.join()
task_spawn.join()
