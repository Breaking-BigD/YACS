from matplotlib import pyplot as plt
from datetime import datetime
import statistics
import os
import re


def calc_mean_median(d, t):
    completion_time = []
    for i in d:
        completion_time.append(
            (d[i]['end_time'] - d[i]['start_time']).total_seconds())
    print(t, ':', statistics.mean(completion_time))
    print(t, ':', statistics.median(completion_time))


def generate_jobs_tasks(log_file):
    f = open(log_file, 'r')
    task_dict = dict()
    job_dict = dict()
    worker_dict = dict()
    task_count = {}
    for i in f.readlines():
        data = i.split()
        time = datetime.strptime(
            data[0] + ' ' + data[1], '%Y-%m-%d %H:%M:%S,%f')
        if(data[2] == 'Started_Job->'):
            job_dict[data[4]] = {}
            job_dict[data[4]]['start_time'] = time
        elif(data[2] == 'Completed_Job->'):
            job_dict[data[4]]['end_time'] = time
        elif(data[2] == 'Started_Task->'):
            task_dict[data[6]] = {}
            task_dict[data[6]]['start_time'] = time
            if(data[-1] not in worker_dict):
                worker_dict[data[-1]] = [(time, 1)]
            else:
                worker_dict[data[-1]
                            ].append((time, worker_dict[data[-1]][-1][1]+1))

        elif(data[2] == 'Completed_Task->'):
            task_dict[data[6]]['end_time'] = time
            worker_dict[data[-1]
                        ].append((time, worker_dict[data[-1]][-1][1]-1))
    return {"job": job_dict, "task": task_dict, "worker": worker_dict}


def plot(worker_dict, t):
    plt.title(t)
    for i in worker_dict:
        x = [(j[0]-datetime(1970, 1, 1)).total_seconds()
             for j in worker_dict[i]]
        y = [j[1] for j in worker_dict[i]]
        plt.plot(x, y, label="worker %s" % i)
    plt.legend()
    plt.show()


arr = os.listdir()
file_dict = dict()
for i in arr:
    if(re.match('.*\.log', i)):
        file_dict[i] = generate_jobs_tasks(i)

for elem in file_dict:
    print(elem)
    calc_mean_median(file_dict[elem]["task"], "Task")
    calc_mean_median(file_dict[elem]["job"], "Job")
    print()

for elem in file_dict:
    plot(file_dict[elem]["worker"],elem)