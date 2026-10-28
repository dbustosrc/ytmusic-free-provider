"""Private SMB reader process. Credentials arrive on stdin, never argv or stderr.

Process isolation lets the caller enforce a deadline even if an SMB request
hangs inside synchronous third-party code. No mounts or temporary media files.
"""

import hashlib
import errno
import json
import ntpath
import re
import sys


def read(config, client, output):
    entry = config["entry"]
    video_id = entry["video_id"]
    filename = entry["filename"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id) or filename not in (
            video_id + ".opus", video_id + ".m4a"):
        raise ValueError("Invalid cache filename")
    path = ntpath.join(config["directory"], filename)
    options = dict(username=config["username"], password=config["password"],
                   connection_timeout=2, connection_cache={}, require_signing=True)

    def report(state, **values):
        output.write(json.dumps({"state": state, **values}).encode() + b"\n")
        output.flush()

    try:
        try:
            stat = client.stat(path, **options)
        except OSError as err:
            if isinstance(err, FileNotFoundError) or err.errno == errno.ENOENT:
                report("missing")
                return
            raise
        if stat.st_size != entry["size"] or not 0 < stat.st_size <= 256 * 1024 * 1024:
            report("invalid")
            return
        expected = entry["metadata"]["sha256"]
        if not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise ValueError("Invalid cache checksum")
        with client.open_file(path, mode="rb", share_access="r", **options) as file:
            if stat.st_mtime_ns != entry["mtime_ns"]:
                digest = hashlib.sha256()
                while part := file.read(65536):
                    digest.update(part)
                after = client.stat(path, **options)
                if digest.hexdigest() != expected or (stat.st_size, stat.st_mtime_ns) != (
                        after.st_size, after.st_mtime_ns):
                    report("invalid")
                    return
                file.seek(0)
            report("ready", size=stat.st_size, mtime_ns=stat.st_mtime_ns)
            size = 0
            digest = hashlib.sha256()
            while part := file.read(65536):
                size += len(part)
                if size > stat.st_size:
                    raise ValueError("Cache changed while reading")
                digest.update(part)
                output.write(part)
                output.flush()
            if size != stat.st_size or digest.hexdigest() != expected:
                raise ValueError("Cache read integrity failure")
    finally:
        client.reset_connection_cache(connection_cache=options["connection_cache"], fail_on_error=False)


if __name__ == "__main__":
    try:
        import logging
        logging.disable(logging.CRITICAL)
        import smbclient
        raw = sys.stdin.buffer.read(131073)
        if len(raw) > 131072:
            raise ValueError("Oversized cache request")
        read(json.loads(raw), smbclient, sys.stdout.buffer)
    except Exception:
        # Never expose a library's exception text or a credential-bearing traceback.
        raise SystemExit(1) from None
