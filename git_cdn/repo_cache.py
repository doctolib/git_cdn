import asyncio
import fcntl
import os
import time
from concurrent.futures import CancelledError

from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web_exceptions import HTTPUnauthorized
from structlog import getLogger

from git_cdn.lock.aio_lock import lock
from git_cdn.log import bind_context_from_exp
from git_cdn.util import GIT_PROCESS_WAIT_TIMEOUT
from git_cdn.util import backoff
from git_cdn.util import ensure_proc_terminated
from git_cdn.util import generate_url
from git_cdn.util import get_bundle_paths
from git_cdn.util import get_subdir

log = getLogger()
BACKOFF_START = float(os.getenv("BACKOFF_START", "0.5"))
BACKOFF_COUNT = int(os.getenv("BACKOFF_COUNT", "5"))
PROGRESS_OPTION = os.getenv("GIT_PROGRESS_OPTION", "--progress")


async def exec_git(*args):
    return await asyncio.create_subprocess_exec(
        "git",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


class RepoCache:
    def __init__(self, path, auth, upstream):
        git_cache_dir = get_subdir("git")
        self.directory = os.path.join(git_cache_dir, path).encode()
        self.auth = auth
        self.lock = self.directory + b".lock"
        self.url = generate_url(upstream, path, auth)
        self.path = path

    def exists(self):
        return os.path.isdir(self.directory)

    def mtime(self):
        if self.exists():
            return os.path.getmtime(self.directory)
        return None

    def utime(self):
        os.utime(self.directory, None)

    def read_lock(self):
        return lock(self.lock, mode=fcntl.LOCK_SH)

    def write_lock(self):
        return lock(self.lock, mode=fcntl.LOCK_EX)

    async def run_git(self, *args):
        """utility which runs a git command, and log outputs
        return stdout, stderr, returncode  via deferred
        """
        t1 = time.time()

        log.debug("git_cmd start", cmd=args)
        stdout_data = b""
        stderr_data = b""
        try:
            git_proc = await exec_git(*args)
            stdout_data, stderr_data = await git_proc.communicate()
        except (
            asyncio.CancelledError,
            CancelledError,
            ConnectionResetError,
        ):
            # on client cancel, keep git command alive until the end to keep the write_lock if taken
            # caution the stdout/stderr before the cancel has been lost
            stdout_data, stderr_data = await git_proc.communicate()
            raise
        finally:
            await ensure_proc_terminated(git_proc, str(args))
            # prevent logging of the creds
            stdout_data = stdout_data.replace(
                self.auth.encode(), self.auth.encode()[:2] + b"<XX>"
            )
            stderr_data = stderr_data.replace(
                self.auth.encode(), self.auth.encode()[:2] + b"<XX>"
            )
            if b"HTTP Basic: Access denied" in stderr_data:
                raise HTTPUnauthorized(reason=stderr_data)

            log.debug(
                "git_cmd done",
                cmd=args,
                stdout_data=stdout_data.decode(errors="replace")[:128],
                stderr_data=stderr_data.decode(errors="replace")[:128],
                rc=git_proc.returncode,
                pid=git_proc.pid,
                cmd_duration=time.time() - t1,
            )
        return stdout_data, stderr_data, git_proc.returncode

    async def fetch(self):
        for timeout in backoff(BACKOFF_START, BACKOFF_COUNT):
            # fetch all refs (including MRs) and tags, and prune if needed
            _, _, returncode = await self.run_git(
                "--git-dir",
                self.directory,
                "fetch",
                PROGRESS_OPTION,
                "--prune",
                "--force",
                "--tags",
                self.url,
                "+refs/*:refs/remotes/origin/*",
                "^refs/pull/*",
            )
            if returncode == 0:
                break
            log.warning("fetch failed, trying again", timeout=timeout)
            await asyncio.sleep(timeout)
        self.utime()

    async def clone(self):
        _, bundle_lock, bundle_file = get_bundle_paths(self.path)
        for timeout in backoff(BACKOFF_START, BACKOFF_COUNT):
            if os.path.exists(bundle_file):
                async with lock(bundle_lock, mode=fcntl.LOCK_SH):
                    # try to clone the bundle file instead
                    _, stderr, returncode = await self.run_git(
                        "clone", PROGRESS_OPTION, "--bare", bundle_file, self.directory
                    )
                    if returncode == 0:
                        break
                    # didn't work? erase that file and retry the clone
                    os.unlink(bundle_file)

            if self.exists():
                rm_proc = await asyncio.create_subprocess_exec(
                    "rm",
                    "-rf",
                    self.directory,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await ensure_proc_terminated(
                    rm_proc, f"rm -rf {self.directory}", timeout=3600
                )
            _, stderr, returncode = await self.run_git(
                "clone", PROGRESS_OPTION, "--bare", self.url, self.directory
            )
            if returncode == 0:
                break
            log.warning("clone failed, trying again", timeout=timeout)
            await asyncio.sleep(timeout)
        if returncode != 0:
            raise HTTPInternalServerError(reason=stderr.decode())

    async def cat_file(self, refs):
        stdout = None
        proc = await asyncio.create_subprocess_exec(
            "git",
            "cat-file",
            "--batch-check",
            "--no-buffer",
            stdout=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.directory,
        )
        try:
            refs_str = b"\n".join(refs) + b"\n"
            stdout, _ = await proc.communicate(refs_str)
        except Exception as e:
            bind_context_from_exp(e)
            log.exception("cat-file failure")
            raise
        finally:
            await ensure_proc_terminated(proc, "git cat-file", GIT_PROCESS_WAIT_TIMEOUT)
            log.debug("cat-file done", pid=proc.pid)
        return stdout

    async def update(self):
        prev_mtime = self.mtime()
        async with self.write_lock():
            if not self.exists():
                await self.clone()
                await self.fetch()
            elif prev_mtime == self.mtime():
                # in case of race condition, it means that we are the first to take the write_lock
                # so we fetch to update the rcache (that will update the mtime too)
                # else, someone took the write_lock before us and so the rcache
                # has been updated already, we do not need to do it
                await self.fetch()

    async def ensure_input_wants(self, wants):
        """Checks if all 'wants'
        and updates rcache if that is not the case
        """
        if not self.exists():
            log.debug("rcache noexistent, cloning")
            await self.update()
        else:
            not_our_refs = True
            async with self.read_lock():
                not_our_refs = await self._missing_want(wants)

            if not_our_refs:
                log.debug("not our refs, fetching")
                await self.update()

    async def _missing_want(self, wants):
        """Return True if at least one sha1 in 'wants' is missing in self.rcache"""
        try:
            stdout = await self.cat_file(wants)
        except FileNotFoundError:
            # Exception while doing git cat command
            # Is rcache really valid ?
            # By returning True, we will ask for an update
            return True

        return b"missing" in stdout

    async def execute_git_command(self, required_sha1s, *args):
        await self.ensure_input_wants(required_sha1s)
        p = await asyncio.create_subprocess_exec(
            "git",
            "--git-dir",
            self.directory,
            *args,
            stderr=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        cmd_stdout, _ = await p.communicate()
        return cmd_stdout
