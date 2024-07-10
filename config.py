# Standard Library
import asyncio
import logging
import os

from prometheus_client import multiprocess

from git_cdn.git_cdn import GUNICORN_WORKER_NB
from git_cdn.util import setup_prometheus_multiproc_dir

# pylint: disable=unused-argument,protected-access

workers = GUNICORN_WORKER_NB
# set a big timeout to avoid worker being killed, and leaking semaphore
timeout = 3600
# gitCDN requests take can be very long, so try to finish them before killing.
graceful_timeout = 60 * 5
# Tentative to avoid connection reset
keepalive = int(os.getenv("GUNICORN_KEEPALIVE", "2"))

# you can try different worker class
# - aiohttp.worker.GunicornWebWorker (default)
# - aiohttp.worker.GunicornUVLoopWebWorker
worker_class = os.getenv("GUNICORN_WORKER_CLASS", "aiohttp.worker.GunicornWebWorker")

errorlog = "-"
loglevel = "debug"

# if None, there won't be any log to structlog, so push it to /dev/null instead
accesslog = "/dev/null"
access_log_format = (
    '%a "%r" %s %b "%{User-Agent}i" "%{X-FORWARDED-FOR}i" '
    '"%{X-CI-JOB-URL}i" "%{X-CI-PROJECT-PATH}i" "%{X-REPO-JOB-URL}i" %D'
)

log = logging.getLogger()
log.setLevel(logging.DEBUG)
# Upload pack Limit with Semaphores

# The default child watcher return a log error:
# "Unknown child process pid 32913, will report returncode 255"
# when the child process is already finished, so using FastChildWatcher to ignore this issue
asyncio.set_child_watcher(asyncio.FastChildWatcher())


# Ensure a clean collector registry directory is available for prometheus metrics when
# the server starts
# https://prometheus.github.io/client_python/multiprocess/
def on_starting(_server):
    if not os.getenv("PROMETHEUS_ENABLED"):
        log.debug(
            "Not starting multiprocess prometheus collector registry: PROMETHEUS_ENABLED is not set"
        )
        return
    setup_prometheus_multiproc_dir()


# Add logs when workers are killed
def worker_int(worker):
    log.error("worker received INT or QUIT signal")

    ## get traceback info
    import sys
    import threading
    import traceback

    id2name = {th.ident: th.name for th in threading.enumerate()}
    code = []
    for threadId, stack in sys._current_frames().items():
        thread_name = id2name.get(threadId, "")
        code.append(f"\n# Thread: {thread_name}({threadId})")
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append(f"File: {filename}, line {lineno}, in {name}")
            if line:
                code.append(f"  {line.strip()}")
    log.warning("\n".join(code))


def worker_abort(worker):
    log.error("worker received SIGABRT signal")


def worker_exit(server, worker):
    log.warning("Worker Exiting")


def child_exit(server, worker):
    if os.getenv("PROMETHEUS_ENABLED"):
        multiprocess.mark_process_dead(worker.pid)
    log.warning("Child Worker exiting")
