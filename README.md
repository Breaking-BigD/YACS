# YACS

* Please view the document **README.pdf** for execution procedure and more details


# Detailed Working of the code:-

I. master.py

	Args - path of config, algo_type
	Worker - nested dictionary
		key:value - worker_id : dictionary
			keys - max_slots,port_no,free_slots

	workerIndex - sorted list of workers

	Threads Used
		job_request_handler - listens for incoming job requests from request.py
													on port no 5000
			- separate thread, so runs parallelly
			- run method overrides default run()
			- run() calls job_object_maker()
				- newJob - nested dictionary
					key:value - remaining_map_tasks : len of map_tasks
								 remaining_reduce_tasks : len of reduce_task
								 map_tasks : nested dictionary
								 	key:value - task_id : dictionary
										keys - worker assigned, status {-1(not yet proc),0(processing),1(done)}
														type of task, job_id of the task
								reduce_tasks - similar to map_tasks
				- Log starting of Job
				- Jobs - dictionary
					key:value - job_id : newJob dict --------- locked
				- TaskPool - list
					append all map tasks to TaskPool --------- locked
			- close connection, listen for new one

		task_handler - maps worker to task based on algo
			- separate thread, so runs parallelly
			- run() method:-
				- pops a task from TaskPool, stores in new_task -------- locked
				- Based on type of algo, allocates worker if it has slots available
				- free_worker - assigned worker by algo
				- Decrement no of free slots on chosen worker -------- locked
				- Change status of task and assigned worker in Jobs dict
				- Connect to selected worker, send new_task
				- Log starting of Task
				- Close Connection

		worker_response_handler - receives task completion status from worker
			- separate thread, so runs parallelly
			- run() method:-
				- receives response from worker on socket 5001
				- response contains info about all completed tasks at that instant
				- It logs the completion of tasks
				- Increment no of free slots on worker -------- locked
				- Change status of the executed task to finished --- 1
				- Decrement count of remaining tasks
				- If all tasks in a Job are complete,
				 	- log completion of Job ----------- locked
				- If all map tasks of a Job are complete,
					- append all reduce tasks to TaskPool ---------- locked
				- Close connection, listen for new responses

	start - implicitly calls the run method
	join - main thread which spawns threads waits for threads to terminate

II. worker.py

	Args - req_port,slots
	execution_slots - list of dict - new_task, initially None x len(total_slots)

	Threads Used:-
	slot_handler() - receives scheduled task from worker
		- new_task - dict containing task info, rec from worker
		- find a free slot and allocate to task ---------- locked
		- close connection with master, listen for more incoming requests

	task_executer() - sends task completion status to master
		- message_list - list of completed tasks(dicts)
		- If a given slot is assigned to a task,
			- Decrement the duration, if task completed, add to message_list ---- locked
		- Sleep for 1 sec, since duration is in secs
		- if there are completed tasks, send their info to master on port 5001
		- close connection

III. analyzer.py

	reads from logs

	generate_jobs_tasks()
		task_dict - stores info about each task
		job_dict - stores info about each job
		worker_dict - stores info about each worker

		- Reads timestamps(Start/End) of job/task, job_id, task_id
		- Stores info about each task, job, worker in dict

	calc_mean_median() - calculates mean, median after calculating time required
												for completion (End - Start)

	task_2() - Plots graphs
