# MapReduce Distributed System Project

## Overview

This project implements a MapReduce distributed system, including worker nodes that execute map and reduce tasks and a coordinator node that manages task assignments and monitors task completion. The system is designed to handle large-scale data processing tasks efficiently and fault-tolerantly.

![MapReduce System Architecture](diagram.png)

## Features

### Map Phase

- **Intermediate Data Division**: The map phase divides intermediate keys into buckets for `nReduce` reduce tasks, where `nReduce` is the number of reduce tasks.
- **Intermediate Data File Format**: `mr-out-map_task_id-ihash%nReduce`; the last part determines which worker will execute the corresponding reduce task.

### Workers

- **Communication**: Workers communicate with the coordinator via RPC.
- **Task Handling**:
  - **Request Tasks**: Workers ask the coordinator for tasks.
  - **Execute Tasks**: Workers read task input from one or more files, execute the task, and write the task’s output to one or more files.
  - **Report Status**: Workers report their task state (MAP/REDUCE) to the coordinator.
- **Functionalities**:
  - Ask the coordinator for tasks.
  - Report task status to the coordinator.
  - Execute map and reduce tasks.

### Coordinator

- **Task Management**:
  - Reassigns tasks if a worker hasn’t completed its task within a reasonable amount of time (ten seconds).
- **Communication**:
  - Only replies to workers to assign map or reduce tasks.

## RPC Design

### Message Types

- **Types**:

  - **RequestTask**: Worker requests a task from the coordinator.
  - **AssignMapTask**: Coordinator assigns a map task to the worker.
  - **AssignReduceTask**: Coordinator assigns a reduce task to the worker.
  - **SucceedMapTask**: Worker reports a successful map task.
  - **FailMapTask**: Worker reports a failed map task.
  - **SucceedReduceTask**: Worker reports a successful reduce task.
  - **FailReduceTask**: Worker reports a failed reduce task.
  - **ShutdownWorker**: Coordinator tells the worker to shut down.
  - **HoldWorker**: Coordinator tells the worker to hold on.

- **Worker Requests**:
  - Workers request tasks from the coordinator.
  - Workers return map and reduce results.
- **Coordinator Responses**:
  - Coordinator assigns map or reduce tasks to workers.
  - Coordinator informs workers to become idle or exit.
