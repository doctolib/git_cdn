import os
import time
import pathlib
import hashlib
from structlog import getLogger
from git_cdn.util import WORKDIR

log = getLogger()

class AuthCache:
    def __init__(self):
        self.cache_ttl = int(os.getenv("AUTH_CACHE_TTL", 0))
        self.directory = os.path.join(WORKDIR, 'auth_cache')
        pathlib.Path(self.directory).mkdir(parents=True, exist_ok=True)

    def auth_ok(self, auth_header, path):
        if self.cache_ttl <= 0:
            return False
        if not auth_header or auth_header == "":
            return False
        filename = self.cache_file(auth_header, path)
        try:
            stats = os.stat(filename)
            now = time.time()
            if (now - stats.st_mtime) > self.cache_ttl:
                log.info(f"Auth cache expired: {filename}")
                os.unlink(filename)
                return False
            log.info(f"Auth cache ok: {filename}")
            return True
        except FileNotFoundError:
            return False

    def store_auth_ok(self, auth_header, path):
        if self.cache_ttl <= 0:
            return False
        if not auth_header or auth_header == "":
            return False
        filename = self.cache_file(auth_header, path)
        with open(filename, 'wb') as f:
            f.write(b"")
        log.info(f"Auth cache created: {filename}")

    def cache_file(self, auth_header, path):
        return os.path.join(self.directory, hashlib.md5(f"{auth_header} {path}".encode('utf-8')).hexdigest())
