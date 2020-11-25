import json
import numpy as np 
import socket
import time

class task_executer(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while 1:
            if len(execution_pool):
                pop_list = [] 
                for i in len(execution_pool):
                    execution_pool[i]["duration"] -= 1
                    if not execution_pool[i]["duration"]:
                        pop_list.append(i)
                time.sleep(1)
                if(len(pop_list)):
                    pop_list = pop_list[::-1]
                    message = [execution_pool.pop(i) for i in pop_list]
                    message = json.dumps(message)
                    masterName = ''
                    masterPort = 5001
                    updateSocket = socket(AF_INET, SOCK_STREAM)
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
            message = master.recv(2048)
            modifiedMessage = message.decode().upper()
            newTask = json.loads(modifiedMessage)
            execution_pool.append(newTask)
            
            master.close()
            

execution_pool = []
slots = int(sys.argv[2])


requestPort = int(sys.argv[1])
requestSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
requestSocket.bind(('', requestPort))
requestSocket.listen(1)