# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
from fastapi import FastAPI, BackgroundTasks
import logging


class BenchmarkWrapperAPI(FastAPI):
    def __init__(self, proc_mapping: WorkloadToProcessMapping, manager: BenchmarkManager):
        super().__init__()
        self.proc_mapping = proc_mapping
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.manager = manager


app = None


@app.get("/task")
def get_task_request():
    return {"benchmark_status": app.manager.status()}


@app.post("/task/{cmd}")
def post_task_request(cmd: str, background_tasks: BackgroundTasks):
    cmd = BenchmarkCommand(cmd)
    if cmd == BenchmarkCommand.PREPARE:
        manager = proc_mapping.map(BenchmarkCommand.PREPARE)
        background_tasks.add_task(manager.run)


    return admission_validation(uid, period_value)
