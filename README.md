# mit-6.824-mapreduce
Mapreduce implementation for mit distributed system (6.824) lab 1: https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

# Design:
1. iterate over the files
2. store status in a map.
3. figure out representation for map and reduce tasks
4. implement "get available task" rpc method - for both map and reduce tasks
   each map & reduce task keeps track of the workers and their status.
5. implement "task done" rpc method - for communicating to coordinate that work is done

Q: where is the worker pool maintained???
ans: in the coordinator

Q: how should the communication model work between coordinator and worker?
   -  get_task: worker->coordinator
   - task_done: worker->coordinator
   -  health_check: coordinator->worker but simplified to just waiting for 10s
   
Q: how do i know if the intermediate file is done generated for a single reduce task?
   - all using files: mr-X-Y is the output of map task X for reduce task Y.
   - coordinator will periodically check if all map tasks for a given reduce shard is complete.
   
Q: where does the sorting of immediate data happen?
   - should happen in the worker. ideally there is a shuffling worker whose job is sorting.

