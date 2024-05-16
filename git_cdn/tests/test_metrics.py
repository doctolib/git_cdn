# Standard Library
import asyncio
import os
import re

import pytest

# Third Party Libraries
from aiohttp.helpers import BasicAuth
from prometheus_client import REGISTRY

from git_cdn import app as git_cdn_app
from git_cdn.conftest import CREDS
from git_cdn.conftest import GITSERVER_UPSTREAM
from git_cdn.conftest import MANIFEST_PATH
from git_cdn.pack_cache import PackCache
from git_cdn.pack_cache import PackCacheCleaner
from git_cdn.tests.test_pack_cache import cache_pack


def get_metric(
    metric_name: str, labels: dict[str, str] = None, default=0, metrics=None
):
    """Helper function to return the current value of a prometheus metric, with optional defaults"""
    value = None
    if not metrics:
        metrics = list(REGISTRY.collect())
    for metric in metrics:
        for s in metric.samples:
            if s.name == metric_name and ((not labels) or s.labels == labels):
                value = s.value
    if value is None:
        return default
    return value


def get_current_metrics():
    """Freeze a subset of the current metric values for testing"""
    metrics = list(REGISTRY.collect())
    return {
        "upstream_responses_total": int(
            get_metric("git_cdn_upstream_responses_total", metrics=metrics)
        ),
        "requests_total": int(get_metric("git_cdn_requests_total", metrics=metrics)),
        "response_status_total": {
            "200": int(
                get_metric(
                    "git_cdn_response_status_total",
                    labels={"status": "200"},
                    metrics=metrics,
                )
            ),
            "302": int(
                get_metric(
                    "git_cdn_response_status_total",
                    labels={"status": "302"},
                    metrics=metrics,
                )
            ),
            "308": int(
                get_metric(
                    "git_cdn_response_status_total",
                    labels={"status": "308"},
                    metrics=metrics,
                )
            ),
            "401": int(
                get_metric(
                    "git_cdn_response_status_total",
                    labels={"status": "401"},
                    metrics=metrics,
                )
            ),
            "403": int(
                get_metric(
                    "git_cdn_response_status_total",
                    labels={"status": "403"},
                    metrics=metrics,
                )
            ),
            "404": int(
                get_metric(
                    "git_cdn_response_status_total",
                    labels={"status": "404"},
                    metrics=metrics,
                )
            ),
            "500": int(
                get_metric(
                    "git_cdn_response_status_total",
                    labels={"status": "500"},
                    metrics=metrics,
                )
            ),
        },
        "nocache_bytes_sent": {
            "sum": int(get_metric("git_cdn_nocache_bytes_sent_sum", metrics=metrics)),
            "count": int(
                get_metric("git_cdn_nocache_bytes_sent_count", metrics=metrics)
            ),
        },
        "cache_miss_bytes_sent": {
            "sum": int(
                get_metric("git_cdn_cache_miss_bytes_sent_sum", metrics=metrics)
            ),
            "count": int(
                get_metric("git_cdn_cache_miss_bytes_sent_count", metrics=metrics)
            ),
        },
        "cache_hit_bytes_sent": {
            "sum": int(get_metric("git_cdn_cache_hit_bytes_sent_sum", metrics=metrics)),
            "count": int(
                get_metric("git_cdn_cache_hit_bytes_sent_count", metrics=metrics)
            ),
        },
        "total_bytes_sent": {
            "sum": int(get_metric("git_cdn_total_bytes_sent_sum", metrics=metrics)),
            "count": int(get_metric("git_cdn_total_bytes_sent_count", metrics=metrics)),
        },
        "pack_cache_evicted_bytes": {
            "sum": int(
                get_metric("git_cdn_pack_cache_evicted_bytes_sum", metrics=metrics)
            ),
            "count": int(
                get_metric("git_cdn_pack_cache_evicted_bytes_count", metrics=metrics)
            ),
        },
        "pack_sent_bytes_total": int(
            get_metric("git_cdn_pack_sent_bytes_total", metrics=metrics)
        ),
        "pack_cache_used_bytes": int(
            get_metric("git_cdn_pack_cache_used_bytes", metrics=metrics)
        ),
    }


@pytest.mark.asyncio
async def test_upstream_redirect(make_client, cdn_event_loop):
    assert cdn_event_loop
    os.environ["PROMETHEUS_ENABLED"] = "true"
    # Use make_app() directly instead of fixture here so we can set PROMETHEUS_ENABLED first
    app = git_cdn_app.make_app(GITSERVER_UPSTREAM)
    client = await make_client(app)
    start_metrics = get_current_metrics()

    resp = await client.get(
        "/redirect_me_please", auth=BasicAuth(*CREDS.split(":")), allow_redirects=False
    )
    assert resp.status == 302

    end_metrics = get_current_metrics()
    assert (
        end_metrics["upstream_responses_total"]
        - start_metrics["upstream_responses_total"]
        == 1
    ), "git_cdn_upstream_responses_total was not updated after get request"
    assert (
        end_metrics["requests_total"] - start_metrics["requests_total"] == 1
    ), "git_cdn_requests_total was not updated after get request"
    assert (
        end_metrics["response_status_total"]["302"]
        - start_metrics["response_status_total"]["302"]
        == 1
    ), 'git_cdn_response_status_total{status="302"} was not updated after get request'


@pytest.mark.asyncio
async def test_pack_cache_clean_metrics(tmpworkdir, cdn_event_loop):
    assert cdn_event_loop
    assert tmpworkdir

    start_eviction_count = int(get_metric("git_cdn_pack_cache_evicted_bytes_count"))

    # Add one entry to the cache pack. Note: the test cache_pack() function already
    # calls send_pack() once, so no need to call it again.
    pc1: PackCache = await cache_pack("1")

    cleaner = PackCacheCleaner()
    # Trigger the cleaner with no evictions to update the cache_size metrics
    cleaner.max_size = 1000000000
    assert cleaner.clean_task() == 0
    start_pack_cache_size = int(get_metric("git_cdn_pack_cache_used_bytes"))
    assert start_eviction_count == int(
        get_metric("git_cdn_pack_cache_evicted_bytes_count")
    ), """PackCacheCleaner should not increment git_cdn_pack_cache_evicted_bytes metric
        when no entries are evicted"""
    assert start_pack_cache_size == int(
        pc1.size()
    ), """git_cdn_pack_cache_used_bytes should reflect the size of the pack cache
        after PackCacheCleaner.clean_task()"""
    # Now evict the pack cache entry and check the cache_size metric is updated
    cleaner.max_size = 1
    assert cleaner.clean_task() == 1

    end_eviction_count = int(get_metric("git_cdn_pack_cache_evicted_bytes_count"))
    assert (
        end_eviction_count - start_eviction_count == 1
    ), "PackCacheCleaner() did not increment git_cdn_pack_cache_evicted_bytes_count"
    end_pack_cache_size = int(get_metric("git_cdn_pack_cache_used_bytes"))
    assert (
        end_pack_cache_size == 0
    ), "git_cdn_pack_cache_used_bytes should reflect an empty pack cache after evicting all entries"


@pytest.mark.asyncio
async def test_local_clone_metrics(make_client, cdn_event_loop, tmpdir, header_for_git):
    assert cdn_event_loop
    os.environ["PROMETHEUS_ENABLED"] = "true"
    # Use make_app() directly instead of fixture here so we can set PROMETHEUS_ENABLED first
    app = git_cdn_app.make_app(GITSERVER_UPSTREAM)
    client = await make_client(app)

    start_metrics = get_current_metrics()
    url = f"{client.baseurl}/{MANIFEST_PATH}"
    repo_dir_1 = os.path.join(tmpdir.strpath, "test_clone_1")
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        "protocol.version=2",
        "clone",
        # "--depth=1",
        url,
        repo_dir_1,
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0
    # Basic git-clone should result in at least three requests:
    # 1. 401 response to /info/refs with no auth
    # 2. 200 response to /info/refs with auth
    # 3. one or more 200 responses to /git-upload-pack with the requested refs
    after_metrics = get_current_metrics()
    assert (
        after_metrics["upstream_responses_total"]
        - start_metrics["upstream_responses_total"]
        > 2
    )
    assert after_metrics["requests_total"] - start_metrics["requests_total"] > 2
    assert (
        after_metrics["response_status_total"]["200"]
        - start_metrics["response_status_total"]["200"]
        >= 2
    )
    total_bytes = (
        after_metrics["total_bytes_sent"]["sum"]
        - start_metrics["total_bytes_sent"]["sum"]
    )
    nocache_bytes = (
        after_metrics["nocache_bytes_sent"]["sum"]
        - start_metrics["nocache_bytes_sent"]["sum"]
    )
    miss_bytes = (
        after_metrics["cache_miss_bytes_sent"]["sum"]
        - start_metrics["cache_miss_bytes_sent"]["sum"]
    )
    hit_bytes = (
        after_metrics["cache_hit_bytes_sent"]["sum"]
        - start_metrics["cache_hit_bytes_sent"]["sum"]
    )
    assert (
        total_bytes == nocache_bytes + miss_bytes + hit_bytes
    ), """git_cdn_cache_bytes_sent_sum metric should always be
        git_cdn_nocache_bytes_sent_sum + git_cdn_cache_miss_bytes_sent_sum
        + git_cdn_cache_hit_bytes_sent_sum"""

    # When fetching the same ref again, a subsequent fetch should exclusively hit the pack cache
    # for node in os.listdir(prom_multiproc_dir):
    repo_dir_2 = os.path.join(tmpdir.strpath, "test_clone_2")
    proc_2 = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        "protocol.version=2",
        "clone",
        # "--depth=1",
        url,
        repo_dir_2,
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc_2.wait()) == 0

    end_metrics = get_current_metrics()
    cached_hit_bytes = (
        end_metrics["cache_hit_bytes_sent"]["sum"]
        - after_metrics["cache_hit_bytes_sent"]["sum"]
    )
    assert (
        cached_hit_bytes == miss_bytes
    ), """git_cdn_cache_hit_bytes_sent_sum and git_cdn_cache_miss_bytes_sent_sum
        should be identical for a cached vs. missed request"""
    assert (
        end_metrics["cache_miss_bytes_sent"]["count"]
        == after_metrics["cache_miss_bytes_sent"]["count"]
    ), "git_cdn_cache_miss_bytes_sent_count should not increment when fetching a cached pack"


@pytest.mark.asyncio
async def test_get_metrics_route(make_client, cdn_event_loop):
    assert cdn_event_loop
    os.environ["PROMETHEUS_ENABLED"] = "true"
    # Use make_app() directly instead of fixture here so we can set PROMETHEUS_ENABLED first
    app = git_cdn_app.make_app(GITSERVER_UPSTREAM)
    client = await make_client(app)
    resp = await client.get("/metrics", auth=BasicAuth(*CREDS.split(":")))
    assert resp.status == 200
    assert resp.content_type == "text/plain"
    resp_body = await resp.text()
    assert "# HELP git_cdn_requests_total total requests served" in resp_body
    assert "# TYPE git_cdn_pack_cache_used_bytes gauge" in resp_body
    assert re.search(r"git_cdn_upstream_responses_total (\d+.\d+)", resp_body)


@pytest.mark.asyncio
async def test_prometheus_enabled_set_to_false(make_client, cdn_event_loop):
    assert cdn_event_loop
    os.environ["PROMETHEUS_ENABLED"] = "false"
    # Use make_app() directly instead of fixture here so we can set PROMETHEUS_ENABLED first
    app = git_cdn_app.make_app(GITSERVER_UPSTREAM)
    client = await make_client(app)
    resp = await client.get("/metrics", auth=BasicAuth(*CREDS.split(":")))
    # No return code test; gitlab responds with a 403 but github responds with an organization
    resp_body = await resp.text()
    assert "# HELP git_cdn_requests_total total requests served" not in resp_body


@pytest.mark.asyncio
async def test_openmetrics_format(make_client, cdn_event_loop):
    assert cdn_event_loop
    os.environ["PROMETHEUS_ENABLED"] = "true"
    app = git_cdn_app.make_app(GITSERVER_UPSTREAM)
    client = await make_client(app)
    resp = await client.get(
        "/metrics",
        auth=BasicAuth(*CREDS.split(":")),
        headers={"Accept": "application/openmetrics-text"},
    )
    assert resp.status == 200
    assert resp.content_type == "application/openmetrics-text"
