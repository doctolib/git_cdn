# Standard Library
import asyncio
import os
from concurrent.futures import CancelledError
from time import time

# Third Party Libraries
from aiohttp.abc import AbstractStreamWriter
from structlog import getLogger
from structlog.contextvars import bind_contextvars
from structlog.contextvars import get_contextvars

from git_cdn.log import bind_context_from_exp
from git_cdn.pack_cache import PackCache
from git_cdn.pack_cache import PackCacheCleaner
from git_cdn.packet_line import to_packet
from git_cdn.repo_cache import RepoCache
from git_cdn.util import GIT_PROCESS_WAIT_TIMEOUT
from git_cdn.util import ensure_proc_terminated

log = getLogger()

cache_cleaner = PackCacheCleaner()


async def write_input(proc, input_data):
    try:
        proc.stdin.write(input_data)
        await proc.stdin.drain()
    except RuntimeError:
        log.exception("exception while writing to upload-pack stdin")
        raise
    except BrokenPipeError:
        # This occur with large input, and upload pack return an early error
        # like "not our ref"
        log.warning("Ignoring BrokenPipeError, while writing to stdin", pid=proc.pid)
    finally:
        proc.stdin.close()


def input_to_ctx(dict_input):
    if "wants" in dict_input:
        del dict_input["wants"]
    if "haves" in dict_input:
        del dict_input["haves"]
    if "caps" in dict_input:
        del dict_input["caps"]
    bind_contextvars(input_details=dict_input)


class UploadPackHandler:
    """Unit testable upload-pack handler
    which automatically call git fetch to update the local copy"""

    def __init__(
        self,
        path,
        writer: AbstractStreamWriter,
        auth,
        upstream,
        protocol_version,
        sema=None,
    ):
        self.upstream = upstream
        self.auth = auth
        self.path = path
        self.sema = sema
        self.writer = writer
        self.rcache = None
        self.pcache = None
        self.pcache_hit = False
        self.rcache_hit = False
        self.protocol_version = protocol_version

    async def _do_upload_pack(self, data):
        proc = await asyncio.create_subprocess_exec(
            "git-upload-pack",
            "--stateless-rpc",
            self.rcache.directory,
            env=dict(os.environ, GIT_PROTOCOL=f"version={self.protocol_version}"),
            stdout=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            if self.pcache:
                await asyncio.gather(
                    write_input(proc, data.input),
                    asyncio.shield(
                        self.pcache.cache_pack(proc.stdout.readexactly, self.writer)
                    ),
                )
            else:
                await asyncio.gather(
                    write_input(proc, data.input),
                    self._flush_to_writer(proc.stdout.read),
                )
        except (asyncio.CancelledError, CancelledError, ConnectionResetError) as e:
            bind_context_from_exp(e)
            log.warning("Client disconnected during upload-pack")
            raise
        except Exception:
            log.exception("upload pack failure")
            raise
        finally:
            # Wait 10 min, for the shielded upload pack to terminate
            # or 2s if not caching, as the process is useless now
            timeout = 10 * 60 if self.pcache else GIT_PROCESS_WAIT_TIMEOUT
            await ensure_proc_terminated(proc, "git upload-pack", timeout)
            if proc.returncode != 0:
                error_message = await proc.stderr.read()
                bind_contextvars(
                    upload_pack_status="error",
                    upload_pack_returncode=proc.returncode,
                    reason=error_message,
                )
                await self._write_pack_error(error_message.decode())

            log.debug("Upload pack done", pid=proc.pid)

    async def _write_pack_error(self, error: str):
        log.error("Upload pack, sending error to client", pack_error=error)
        pkt = to_packet(("ERR " + error).encode())
        await self.writer.write(pkt)

    async def _flush_to_writer(self, read_func):
        CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 32 * 1024))
        while True:
            chunk = await read_func(CHUNK_SIZE)
            if not chunk:
                break
            await self.writer.write(chunk)

    async def _run_with_cache(self, parsed_input):
        self.pcache = PackCache(parsed_input.hash)
        async with self.pcache.read_lock():
            if self.pcache.exists():
                self.pcache_hit = True
                await self.pcache.send_pack(self.writer)
                return

        async with self.pcache.write_lock():
            # In case 2 threads race for write lock, check again if it has been added in the cache
            if not self.pcache.exists():
                await self._execute(parsed_input)

        async with self.pcache.read_lock():
            if self.pcache.exists():
                await self.pcache.send_pack(self.writer)
                # ensure cache size doesn't grow in a background task
                cache_cleaner.clean()
                return
        # if we are here because of upload_pack failure,
        # the client see the error via the git protocol (mainly "not our ref" error)
        # and "Response stats" report the error via context upload_pack_status="error"
        if get_contextvars()["upload_pack_status"] != "error":
            # Should not happen
            # This case may be due to pack cache deletion before serving it:
            # look logs with with the corresponding hash
            raise RuntimeError("Run with cache failed")

    async def _upload_pack(self, parsed_input):
        async with self.rcache.read_lock():
            if self.rcache.exists():
                if not self.sema:
                    await self._do_upload_pack(parsed_input)
                else:
                    start_wait = time()
                    async with self.sema:
                        start_upload_pack = time()
                        bind_contextvars(sema_wait=start_upload_pack - start_wait)
                        await self._do_upload_pack(parsed_input)
                        bind_contextvars(
                            upload_pack_duration=time() - start_upload_pack
                        )

    async def _missing_want(self, wants):
        """Return True if at least one sha1 in 'wants' is missing in self.rcache"""
        try:
            stdout = await self.rcache.cat_file(wants)
        except FileNotFoundError:
            # Exception while doing git cat command
            # Is rcache really valid ?
            # By returning True, we will ask for an update
            return True

        return b"missing" in stdout

    async def _ensure_input_wants_in_rcache(self, wants):
        """Checks if all 'wants' are in rcache
        and updates rcache if that is not the case
        """
        if not self.rcache.exists():
            log.debug("rcache noexistent, cloning")
            await self.rcache.update()
        else:
            not_our_refs = True
            async with self.rcache.read_lock():
                not_our_refs = await self._missing_want(wants)

            if not_our_refs:
                log.debug("not our refs, fetching")
                await self.rcache.update()
            else:
                self.rcache_hit = True

    async def _execute(self, parsed_input):
        """Runs git upload-pack
        after being insure that all 'wants' are in cache
        """
        self.rcache = RepoCache(self.path, self.auth, self.upstream)

        await self._ensure_input_wants_in_rcache(parsed_input.wants)
        await self._upload_pack(parsed_input)

    async def run(self, parsed_input):
        """Run the whole process of upload pack, including sending the result to the writer"""
        dict_input = parsed_input.as_dict.copy()
        log.debug("parsed input", input_details=dict_input)
        input_to_ctx(dict_input)
        if parsed_input.parse_error:
            await self._write_pack_error(
                f"Wrong upload pack input: {parsed_input.input[:128]}"
            )
            return
        if not parsed_input.wants:
            log.warning("Request without wants")
            return
        if parsed_input.can_be_cached():
            await self._run_with_cache(parsed_input)
        else:
            await self._execute(parsed_input)
