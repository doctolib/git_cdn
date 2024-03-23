# Standard Library
import asyncio
import base64
import os
import re
import urllib
from asyncio.subprocess import Process
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version

# Third Party Libraries
from aiohttp.web_exceptions import HTTPBadRequest
from structlog import getLogger

WORKDIR = os.path.expanduser(os.getenv("WORKING_DIRECTORY", "/tmp/workdir"))
GITLFS_OBJECT_RE = re.compile(r"(?P<path>.*\.git)/gitlab-lfs/objects/[0-9a-f]{64}$")
GIT_PROCESS_WAIT_TIMEOUT = int(os.getenv("GIT_PROCESS_WAIT_TIMEOUT", "2"))
KILLED_PROCESS_TIMEOUT = 30

try:
    GITCDN_VERSION = version("git_cdn")
except PackageNotFoundError:
    GITCDN_VERSION = "unknown"

log = getLogger()


def check_path(path):
    if path.startswith("/"):
        raise HTTPBadRequest(reason="bad path: " + path)
    if "/../" in path or path.startswith("../"):
        raise HTTPBadRequest(reason="bad path: " + path)


def find_gitpath(path):
    """find the git path for this url path
    ensures it ends with ".git", and do not start with "/" and do not contain ../
    """
    path = path.strip("/")
    check_path(path)

    for suffix in (
        ".git/info/refs",
        ".git/git-upload-pack",
        ".git/git-receive-pack",
        "/info/refs",
        "/git-upload-pack",
        "/git-receive-pack",
        ".git/clone.bundle",
        "/clone.bundle",
        "/info/lfs/objects/batch",
    ):
        if path.endswith(suffix):
            return path[: -len(suffix)] + ".git"
    res = GITLFS_OBJECT_RE.match(path)
    if res:
        return res.groupdict()["path"]
    return None


def get_subdir(subpath):
    """find or create the working directory of the repository path"""
    d = os.path.join(WORKDIR, subpath)
    if not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)
    return d


def get_bundle_paths(git_path):
    """compute the locks and bundle paths"""
    git_path = git_path.rstrip("/")
    assert git_path.endswith(".git")
    bundle_dir = get_subdir("bundles")
    bundle_name = os.path.basename(git_path)[:-4]  # remove ending '.git'
    lock = os.path.join(bundle_dir, bundle_name + ".lock")
    bundle_file = os.path.join(bundle_dir, bundle_name + "_clone.bundle")
    return bundle_name, lock, bundle_file


def backoff(start, count):
    """
    Return generator of backoff retry with factor of 2

    >>> list(backoff(0.1, 5))
    [0.1, 0.2, 0.4, 0.8, 1.6]
    """
    for x in range(count):
        yield start * 2**x


def get_url_creds_from_auth(auth):
    # decode the creds from the auth in
    creds = base64.b64decode(auth.split(" ", 1)[-1]).decode()
    # gitlab token not supposed to require quote, but we still encode.
    # because some people put their email as user and this put an extraneous @ in the url.
    # https://tools.ietf.org/html/rfc7617  -> BasicAuth  Page 4
    # https://tools.ietf.org/html/rfc3986.html -> URLs:  3.2.1.  User Information
    return ":".join([urllib.parse.quote_plus(p) for p in creds.split(":", 1)])


def generate_url(base, path, auth=None):
    url = base + path
    if auth:
        for proto in "http", "https":
            url = url.replace(proto + "://", proto + "://" + auth + "@")
    return url


def log_proc_if_error(proc: Process, cmd: str):
    if not proc.returncode:
        return
    # pylint: disable = protected-access
    cmd_stderr = proc.stderr._buffer.decode() if proc.stderr else ""
    try:
        # we might be in the middle of upload-pack so the stdout might be binary
        cmd_stdout = proc.stdout._buffer.decode() if proc.stdout else ""
    except UnicodeDecodeError:
        cmd_stdout = "<binary>"
    # pylint: enable = protected-access

    # Error 128 on upload-pack is a known issue of git upload-pack and shall be ignored on
    # ctx['depth']==True and ctx['done']==False :
    # https://www.mail-archive.com/git@vger.kernel.org/msg90066.html
    log.info(
        "subprocess return an error",
        cmd=cmd,
        cmd_stderr=cmd_stderr[:128],
        cmd_stdout=cmd_stdout[:128],
        pid=proc.pid,
        returncode=proc.returncode,
    )


async def wait_proc(proc: Process, cmd: str, timeout: int):
    try:
        if proc.returncode is None:
            await asyncio.wait_for(proc.wait(), timeout=timeout)

        log_proc_if_error(proc, cmd)
        return True
    except asyncio.TimeoutError:
        pass
    return False


async def ensure_proc_terminated(
    proc: Process, cmd: str, timeout=GIT_PROCESS_WAIT_TIMEOUT
):
    if await wait_proc(proc, cmd, timeout):
        return
    log.error(
        "process didn't exit, terminate it", cmd=cmd, pid=proc.pid, timeout=timeout
    )
    proc.terminate()
    if await wait_proc(proc, cmd, KILLED_PROCESS_TIMEOUT):
        return
    log.error("process didn't exit, kill it", cmd=cmd, pid=proc.pid, timeout=timeout)
    proc.kill()
    if await wait_proc(proc, cmd, KILLED_PROCESS_TIMEOUT):
        return
    log.error("Process didn't exit after kill", cmd=cmd, pid=proc.pid, timeout=timeout)


def object_module_name(o):
    fn = ""
    if hasattr(o, "__module__"):
        fn = o.__module__ + "."
    fn += o.__class__.__name__
    return fn

def remove_git_credentials(args):
    args_without_credentials = []
    for e in args:
        if re.match(r"((git|ssh|http(s)?)|(git@[\w\.]+))(:(//)?)([\w\.@\:/\-~]+)(\.git)(/)?", str(e)):
            args_without_credentials.append(re.sub("(?<=:)([^@:]+)(?=@[^@]+$)", "*****",str(e)))
        else:
            args_without_credentials.append(str(e))
    return args_without_credentials