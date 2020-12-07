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
    ans=[statistics.mean(completion_time),statistics.median(completion_time)]
    print(t, 'mean :',ans[0])
    print(t, 'median :',ans[1])
    return ans


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


def task_2(worker_dict, t,all_in_one=False):
    if(all_in_one):
        plt.title(t)
        plt.xlabel('Time (HH-MM-SS)')
        plt.ylabel('Number of Tasks')
    else:
        fig,lis=plt.subplots(1,3)
        fig.suptitle(t)
        fig.text(0.04, 0.5, 'Number of Tasks', va='center', rotation='vertical')
        fig.text(0.45,0.04, 'Time (HH-MM-SS)', va='center', rotation='horizontal')
        idx=0
    for i in worker_dict:
        x = [j[0] for j in worker_dict[i]]
        y = [j[1] for j in worker_dict[i]]
        if(all_in_one):
            plt.step(x, y, label="worker %s" % i)
        else:
            lis[idx].step(x,y)
            lis[idx].title.set_text("worker "+i)
            idx+=1
    if(all_in_one):
        plt.legend()
    plt.show()


arr = os.listdir()
file_dict = dict()
for i in arr:
    if(re.match('.*\.log', i)):
        file_dict[i] = generate_jobs_tasks(i)
idx=0
fig,lis=plt.subplots(1,3)
fig.text(0.04, 0.5, 'Time (Seconds)', va='center', rotation='vertical')
for elem in file_dict:
    print(elem)
    t=calc_mean_median(file_dict[elem]["task"], "Task")
    j=calc_mean_median(file_dict[elem]["job"], "Job")
    lis[idx].bar(['Mean Task','Median Task','Mean Job','Median Job'],t+j)
    lis[idx].title.set_text(elem)
    idx+=1
    print()
plt.show()

for elem in file_dict:
    task_2(file_dict[elem]["worker"],elem,all_in_one=True)  