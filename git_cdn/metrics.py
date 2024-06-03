import os
import socket
from typing import Callable

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST
from prometheus_client import REGISTRY
from prometheus_client import CollectorRegistry
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Summary
from prometheus_client import generate_latest
from prometheus_client import multiprocess
from prometheus_client.openmetrics import exposition as openmetrics
from structlog import getLogger

log = getLogger()
# Prometheus metric namespace: prepended to every metric name
namespace = "git_cdn"

# Metric definitions here
host_id = os.getenv("POD_NAME", socket.gethostname())
metric_request_time_seconds = Summary(
    "request_time_seconds", documentation="time spent in requests", namespace=namespace
)
metric_requests_total = Counter(
    "requests_total", documentation="total requests served", namespace=namespace
)
metric_response_status_total = Counter(
    "response_status_total",
    documentation="total requests served by response status",
    labelnames=["status"],
    namespace=namespace,
)
metric_response_status_total.labels("200")
metric_total_bytes_sent = Summary(
    "total_bytes_sent",
    documentation="total traffic served to clients",
    namespace=namespace,
)
metric_upstream_responses_total = Counter(
    "upstream_responses_total",
    documentation="total requests delegated to the upstream without caching",
    namespace=namespace,
)
metric_cache_hit_bytes_sent = Summary(
    "cache_hit_bytes_sent",
    documentation="total bytes served from pack cache hits",
    namespace=namespace,
)
metric_cache_miss_bytes_sent = Summary(
    "cache_miss_bytes_sent",
    documentation="total pack cache bytes served for pack cache misses",
    namespace=namespace,
)
metric_nocache_bytes_sent = Summary(
    "nocache_bytes_sent",
    documentation="total response bytes sent for requests that are not cacheable",
    namespace=namespace,
)
metric_stats_write_seconds = Summary(
    "stats_write_seconds",
    documentation="time spent writing metrics",
    namespace=namespace,
)
metric_workdir_filesystem_avail_bytes = Gauge(
    "workdir_filesystem_avail_bytes",
    documentation="Filesystem free space in bytes",
    multiprocess_mode="liveall",
    labelnames=["gitcdn_hostname"],
    namespace=namespace,
).labels(host_id)
metric_workdir_filesystem_size_bytes = Gauge(
    "workdir_filesystem_size_bytes",
    documentation="Filesystem total size in bytes",
    multiprocess_mode="liveall",
    labelnames=["gitcdn_hostname"],
    namespace=namespace,
).labels(host_id)
metric_pack_cache_evicted_bytes = Summary(
    "pack_cache_evicted_bytes",
    documentation="total data evicted from the pack cache",
    namespace=namespace,
)
metric_pack_sent_bytes = Counter(
    "pack_sent_bytes",
    documentation="total data served directly from pack cache",
    labelnames=["cache_status"],
    namespace=namespace,
)
metric_pack_cache_used_bytes = Gauge(
    "pack_cache_used_bytes",
    documentation="total size of all objects currently in the pack cache",
    labelnames=["gitcdn_hostname"],
    namespace=namespace,
).labels(host_id)
metric_repo_cache_received_bytes = Summary(
    "repo_cache_received_bytes",
    documentation="total data received by repo cache from upstream as reported by git",
    namespace=namespace,
)


def choose_generator(accept_header: str) -> tuple[Callable, str]:
    """
    Return the correct generate function according to *accept_header*.

    Default to the old style.
    """
    accept_header = accept_header or ""
    for accepted in accept_header.split(","):
        if accepted.split(";")[0].strip() == "application/openmetrics-text":
            return (
                openmetrics.generate_latest,
                openmetrics.CONTENT_TYPE_LATEST,
            )

    return generate_latest, CONTENT_TYPE_LATEST


async def serve_metrics(request: web.Request) -> web.Response:
    if not os.getenv("PROMETHEUS_ENABLED", "false").lower() == "true":
        return web.Response(text="prometheus not enabled", status=404)

    # Ref: https://prometheus.github.io/client_python/multiprocess/
    # no coverage, (tests run with single-threaded collector)
    if "PROMETHEUS_MULTIPROC_DIR" in os.environ:  # pragma: no cover
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
    else:
        registry = REGISTRY
    generate, content_type = choose_generator(request.headers.get("Accept"))
    body = generate(registry)
    response_headers = [
        ("Content-Length", str(len(body))),
    ]
    response = web.Response(
        body=body,
        status=200,
        headers=response_headers,
    )
    # This is set separately because aiohttp complains about `;` in
    # content_type thinking it means there's also a charset.
    # cf. https://github.com/aio-libs/aiohttp/issues/2197
    response.content_type = content_type
    return response
