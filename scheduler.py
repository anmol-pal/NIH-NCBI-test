
"""
`scheduler` is a module that executes a number of expensive jobs concurrently,
based on a queue represented by a database table that contains entries with
"state" fields that say when they are ready for execution.

| key | state   | arbitrary config fields ... |
|:---:|:-------:|:----------------------------|
| 001 |  Done   | ... |
| 002 |  Done   | ... |
| 003 | Started | ... |
| 004 | Pending | ... |
| 005 | Pending | ... |
| ... |   ...   | ... |

A machine that's set up to pull jobs from the schedule will use the following
command, for example, to invoke the scheduler:

```sh
scheduler --status-key status --ready-val Pending --max-concurrent-jobs 5
```

You may add additional arguments if they are needed. For the purposes of the
excersize, the actual job the scheduler is running is hard-coded into the
module. You can use the `arbitrary_job` function as a stand-in for it. The
timer delay in the function represents the io bound nature of the job. Set it
to whatever is most useful for testing.

Some additional requirements / considerations:

*   The scheduler must gracefully shut down when the calling process receives
    an interrupt signal.
*   The scheduler must gracefully resolve race conditions if multiple instances
    are running. A job must not be run in more than one instance.

Some bonus questions:

*   How would you optimize the `get_jobs` function?
*   Are there tools that you would use instead of writing this script to manage
    the job scheduling? How would the entire solution change to adopt them?
*   What would you do differently if the job was CPU-bound rather than
    IO-bound? Particularly since Python is not a parallel language (i.e. GIL).
*   How should someone deploying a scheduler-powered job determine their value
    for `--max-concurrent-jobs`?

"""
import argparse
import time
import sys
import asyncio
import signal
import random
from typing import Iterator


STARTED = "Started"
DONE = "Done"
PENDING = "Pending"
job_db = [
    {"key": "001", "state": "Done"},
    {"key": "002", "state": "Done"},
    {"key": "003", "state": "Pending"},
    {"key": "004", "state": "Pending"},
    {"key": "005", "state": "Pending"},
    {"key": "006", "state": "Pending"},
    {"key": "007", "state": "Pending"},
    {"key": "008", "state": "Pending"},
]

class GracefulSchedkiller:
    '''
    Ref: https://stackoverflow.com/questions/18499497/how-to-process-sigterm-signal-gracefully
    '''
    shutdown = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, sig, frame):
        print(f"Received signal {sig}. Shutting down gracefully.")
        self.shutdown = True

    async def shutdown_check(self):
        while not self.shutdown:
            await asyncio.sleep(0.1) 


# region API
async def arbitrary_job(key):
    """
    This is a placeholder for arbitrary, IO- or CPU-bound jobs run using
    subprocess, etc.

    It takes arguments derived from a subset of database columns.

    State is updated as a side effect of the process
    """
    print("Starting Job ID: {}".format(key))
    await asyncio.sleep(random.uniform(2, 8))
    print("Completed Job ID: {}".format(key))


class Job(dict):
    """
    A job object will is a dict wrapping the table entry. Local state is
    fetched when the object is instantiated (not lazily synced).
    Setting keys on the dict will update the value in the dictionary.
    """
    def __init__(self, entry: dict):
        self.entry = entry
        self.key = entry.get("key")
        self.state = entry.get("state")

    def update_state(self, new_state: str):
        self.state = new_state
        for db_entry in job_db:
            if db_entry["key"] == self.key:
                db_entry["state"] = new_state
                break

    async def run(self):
        self.update_state(STARTED)
        await arbitrary_job(self.key)
        self.update_state(DONE)

    def __repr__(self):
        return f"Job(key={self.key}, state={self.state})"


def get_jobs(ready_val) -> Iterator[Job]:
    """
    Yeilds each job in the job table, regardless of status, in order.
    """
    for job in job_db:
        if job['state'] == ready_val:
            yield Job(job)

async def scheduler_run(ready_val, status_key, max_concurrent_jobs ) :
    shutdown_manager = GracefulSchedkiller()
    semphore = asyncio.Semaphore(max_concurrent_jobs)

    async def worker(job):
        async with semphore:
            await job.run()
    async def refil_tasks():
        pending_jobs = list(get_jobs(ready_val))
        tasks = []
        for job in pending_jobs:
            if shutdown_manager.shutdown:
                break
            tasks.append(asyncio.create_task(worker(job)))

        if tasks:
            await asyncio.wait(tasks)

    while not shutdown_manager.shutdown:
        pending = list(get_jobs(ready_val))
        if not pending:
            print("Scheduler found no jobs.. Waiting")
            await asyncio.sleep(5)
            continue
        await refil_tasks()
        await shutdown_manager.shutdown_check()
        print("Scheduler stopped")



def main():
    """
    The main function starts the scheduler with arguments.

    The basic structure here, parsing arguments, and running `scheduler_run`
    can be modified as you will. `max_concurrent_jobs`, `status_key`, and
    `ready_val` are required arguments. You can add others if you think they're
    needed.

    `scheduler_run` is a placeholder for what you will implement. A scheduler
    must fetch the pending jobs and add them for execution.

    It can be a single function as shown below, or you can initialize an object
    here, then have it start running the scheduler.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-concurrent-jobs", action="store", type=int)
    parser.add_argument("--status-key", action="store", type=str, default="status")
    parser.add_argument("--ready-val", action="store", type=str, default="Pending")

    args = parser.parse_args()
    try:
        asyncio.run(scheduler_run(args.ready_val, args.status_key, args.max_concurrent_jobs))
    except Exception as e:
        print(e.with_traceback(None))
        sys.exit(1)


if __name__ == "__main__":
    main()
