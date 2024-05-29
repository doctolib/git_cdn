# -*- coding: utf-8 -*-
# Standard Library
import asyncio
import os

# Third Party Libraries
import aiohttp
import pytest_asyncio
import uvloop
import yarl

import git_cdn.util
from git_cdn import app as git_cdn_app

# pylint: disable=unused-argument,redefined-outer-name,consider-using-f-string,protected-access

GITLAB_REPO_TEST_GROUP = os.getenv("GITLAB_REPO_TEST_GROUP", "grouperenault/repo_test")
GITSERVER_UPSTREAM = os.getenv("GITSERVER_UPSTREAM", "https://gitlab.com/")
MANIFEST_PATH = f"{GITLAB_REPO_TEST_GROUP}/test_git_cdn.git"
CREDS = os.getenv("CREDS", "gitlab-ci-token:{}".format(os.getenv("CI_JOB_TOKEN")))
# For consistency, pytest should not share prometheus metrics across processes
if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
    del os.environ["PROMETHEUS_MULTIPROC_DIR"]


@pytest_asyncio.fixture
def tmpworkdir(tmpdir):
    git_cdn.util.WORKDIR = tmpdir / "gitCDN"
    yield tmpdir


@pytest_asyncio.fixture
def app(tmpworkdir):
    yield git_cdn_app.make_app(GITSERVER_UPSTREAM)


@pytest_asyncio.fixture(scope="module", params=[asyncio, uvloop])
def cdn_event_loop(request):
    if request.param is asyncio:
        loop = asyncio.new_event_loop()
        # FastChildWatcher is failing our tests with following exception
        # RuntimeError: asyncio.get_child_watcher() is not activated,
        #   subprocess support is not installed.
        # Maybe because FastChildWatcher requires a running event loop in the main thread to work
        asyncio.set_child_watcher(asyncio.ThreadedChildWatcher())

    elif request.param is uvloop:
        loop = uvloop.new_event_loop()

    asyncio.set_event_loop(loop)

    yield loop

    if not loop.is_closed():
        loop.close()


class FakeClient:
    def __init__(self, url, creds):
        self.url = yarl.URL(url)
        user, password = creds.split(":")
        self.url = self.url.with_user(user).with_password(password)
        self.baseurl = str(self.url)

    async def get(self, path, **kw):
        if "auth" in kw:
            del kw["auth"]
        url = self.url.join(yarl.URL(path))
        async with aiohttp.ClientSession() as session:
            async with session.get(url, **kw) as r:
                c = await r.content.read()

                async def fake_read():
                    return c

                r.content.read = fake_read
                return r

    async def post(self, path, **kw):
        if "auth" in kw:
            del kw["auth"]
        url = self.url.join(yarl.URL(path))
        async with aiohttp.ClientSession() as session:
            async with session.post(url, **kw) as r:
                c = await r.content.read()

                async def fake_read():
                    return c

                r.content.read = fake_read
                return r


@pytest_asyncio.fixture
def header_for_git(request):
    return ["-c", f"http.extraheader=X-CI-INTEG-TEST: {request.node.nodeid}"]


@pytest_asyncio.fixture
def make_client(aiohttp_client):
    async def ret(app, creds=CREDS):
        if "UNDER_TEST_APP" not in os.environ:
            c = await aiohttp_client(app)
            c.baseurl = f"http://{creds}@localhost:{c._server.port}"
            return c
        return FakeClient(os.environ["UNDER_TEST_APP"], creds)

    yield ret
