import asyncio
import os
import time

import aiohttp
from structlog import getLogger

from git_cdn.metrics import metric_upstream_responses_total
from git_cdn.util import backoff

log = getLogger()


class ClientSessionWithRetry:
    REQUEST_MAX_RETRIES = int(os.getenv("REQUEST_MAX_RETRIES", "10"))

    def __init__(self, get_session, retry_on, method, url, *args, **kwargs):
        self.status_retry_on = retry_on
        self.get_session = get_session
        self.session = None
        self.cm_request = None
        self.method = method
        self.url = url
        self.args = args
        self.kwargs = kwargs

    async def __aenter__(self):
        start_time = time.time()
        self.session = self.get_session()
        self.cm_request = None
        for retries, timeout in enumerate(backoff(0.1, self.REQUEST_MAX_RETRIES)):
            try:
                self.cm_request = await self.session.request(
                    self.method, self.url, *self.args, **self.kwargs
                )
                metric_upstream_responses_total.inc()

                if self.cm_request.status not in self.status_retry_on:
                    return self.cm_request
                log.debug(
                    "upstream wrong return, retry",
                    request_url=self.url,
                    request_method=self.method,
                    resp_status=self.cm_request.status,
                    retries=retries,
                    # only dump the first value in multidict
                    resp_time=time.time() - start_time,
                )
            except aiohttp.ClientConnectionError:
                if retries + 1 >= self.REQUEST_MAX_RETRIES:
                    log.exception(
                        "Max of request retries reached",
                        resp_time=time.time() - start_time,
                        timeout=timeout,
                        request_max_retries=self.REQUEST_MAX_RETRIES,
                        request_url=self.url,
                        request_method=self.method,
                    )
                    raise
                log.exception(
                    "Client connection error",
                    resp_time=time.time() - start_time,
                    timeout=timeout,
                    request_max_retries=self.REQUEST_MAX_RETRIES,
                    retries=retries,
                    request_url=self.url,
                    request_method=self.method,
                )
            await asyncio.sleep(timeout)
        return self.cm_request

    async def __aexit__(self, *args, **kwargs):
        if self.cm_request is not None:
            self.cm_request.close()
