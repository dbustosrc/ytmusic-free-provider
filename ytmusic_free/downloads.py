"""Persistent background downloads. Audio lives exclusively on the SMB server."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager, suppress
from dataclasses import dataclass
import hashlib
import json
import math
import ntpath
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import unquote, urlsplit

LISTEN_SECONDS = 30
MAX_ATTEMPTS = 3
MAX_AUDIO_BYTES = 256 * 1024 * 1024
CHUNK_SIZE = 64 * 1024
_DOWNLOAD_SLOT = asyncio.Semaphore(1)
_VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}\Z")


class DownloadError(Exception):
    """A credential-safe download failure."""


async def blocking(func, *args, **kwargs):
    """Finish a bounded SMB/DB operation before cancellation closes its handles."""
    task = asyncio.create_task(asyncio.to_thread(func, *args, **kwargs))
    try:
        await asyncio.wait((task,))
        return task.result()
    except asyncio.CancelledError:
        with suppress(Exception):
            await task
        raise


@dataclass(frozen=True)
class SMBTarget:
    directory: str
    staging: str
    identity: str

    @classmethod
    def parse(cls, value: str) -> SMBTarget:
        value = value.strip()
        if value.lower().startswith("smb://"):
            url = urlsplit(value)
            if url.username or url.password or url.port or url.query or url.fragment:
                raise ValueError("SMB directory must not contain credentials, port, query or fragment")
            value = "//" + (url.hostname or "") + unquote(url.path)
        parts = value.replace("\\", "/").removeprefix("//").rstrip("/").split("/")
        if not value.startswith(("//", "\\\\")) or len(parts) < 3:
            raise ValueError("Use smb://server/share/Music, including a music subdirectory")
        if any(not p or p in (".", "..") or p.endswith((" ", "."))
               or re.search(r'[\x00-\x1f:*?"<>|]', p) for p in parts):
            raise ValueError("SMB directory contains an unsafe path component")
        if parts[2].casefold() == ".ytmusic-free-staging":
            raise ValueError("The music directory must be separate from download staging")
        directory = "\\\\" + "\\".join(parts)
        identity = hashlib.sha256(directory.casefold().encode()).hexdigest()[:24]
        staging = ntpath.join("\\\\" + "\\".join(parts[:2]), ".ytmusic-free-staging", identity)
        return cls(directory, staging, identity)


class DownloadQueue:
    """One small local state file; no cookies, SMB credentials or audio URLs."""

    def __init__(self, path: Path):
        self.path = path

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                video_id TEXT PRIMARY KEY, metadata TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                next_try REAL NOT NULL DEFAULT 0, error TEXT, created REAL NOT NULL)""")
            db.execute("UPDATE jobs SET status='pending' WHERE status='running'")
            db.execute("""CREATE TABLE IF NOT EXISTS cache_files (
                video_id TEXT PRIMARY KEY, filename TEXT NOT NULL, metadata TEXT NOT NULL,
                size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, validated_at REAL NOT NULL)""")
            # Existing completed jobs predate the cache index. Revalidate by ID in the worker.
            db.execute("""UPDATE jobs SET status='pending', attempts=0, next_try=0
                WHERE status='done' AND video_id NOT IN (SELECT video_id FROM cache_files)""")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        try:
            with db:
                yield db
        finally:
            db.close()

    def enqueue(self, video_id: str, metadata: dict) -> bool:
        if not _VIDEO_ID.fullmatch(video_id):
            raise ValueError("Invalid YouTube video ID")
        with self.connect() as db:
            reopened = db.execute("""UPDATE jobs SET metadata=?,status='pending',attempts=0,next_try=0,error=NULL
                WHERE video_id=? AND status='missing'""", (json.dumps(metadata), video_id))
            result = db.execute(
                "INSERT OR IGNORE INTO jobs(video_id,metadata,created) VALUES(?,?,?)",
                (video_id, json.dumps(metadata, ensure_ascii=False), time.time()),
            )
            return result.rowcount == 1 or reopened.rowcount == 1

    def cached(self, video_id):
        with self.connect() as db:
            row = db.execute("SELECT filename,metadata,size,mtime_ns FROM cache_files WHERE video_id=?",
                             (video_id,)).fetchone()
        if not row:
            return None
        return {"video_id": video_id, "filename": row[0], "metadata": json.loads(row[1]),
                "size": row[2], "mtime_ns": row[3]}

    def remember(self, video_id, filename, metadata, stat):
        with self.connect() as db:
            db.execute("""INSERT INTO cache_files VALUES(?,?,?,?,?,?) ON CONFLICT(video_id)
                DO UPDATE SET filename=excluded.filename,metadata=excluded.metadata,
                size=excluded.size,mtime_ns=excluded.mtime_ns,validated_at=excluded.validated_at""",
                (video_id, filename, json.dumps(metadata, ensure_ascii=False), stat.st_size,
                 stat.st_mtime_ns, time.time()))

    def unavailable(self, video_id, missing=False):
        with self.connect() as db:
            db.execute("DELETE FROM cache_files WHERE video_id=?", (video_id,))
            db.execute("UPDATE jobs SET status=?,error=NULL WHERE video_id=? AND status='done'",
                       ("missing" if missing else "invalid", video_id))

    def claim(self, now: float):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT video_id, metadata, attempts FROM jobs
                WHERE status='pending' AND next_try<=? AND attempts<? ORDER BY created LIMIT 1""",
                (now, MAX_ATTEMPTS)).fetchone()
            if row is None:
                return None
            db.execute("UPDATE jobs SET status='running', attempts=attempts+1 WHERE video_id=?", (row[0],))
            return row[0], json.loads(row[1]), row[2] + 1

    def finish(self, video_id: str):
        with self.connect() as db:
            db.execute("UPDATE jobs SET status='done', error=NULL WHERE video_id=?", (video_id,))

    def fail(self, video_id: str, attempt: int, error: str):
        with self.connect() as db:
            db.execute("UPDATE jobs SET status=?, next_try=?, error=? WHERE video_id=?",
                       ("failed" if attempt >= MAX_ATTEMPTS else "pending",
                        time.time() + 60 * 5 ** (attempt - 1), error, video_id))

    def interrupted(self, video_id: str):
        with self.connect() as db:
            db.execute("UPDATE jobs SET status='pending', attempts=max(0,attempts-1) WHERE video_id=?",
                       (video_id,))


class SMBStore:
    """An isolated SMB session pool, without mounting or local fallback storage."""

    def __init__(self, target: SMBTarget, username: str, password: str, minimum_free: int):
        import smbclient

        self.client = smbclient
        self.target = target
        self.minimum_free = minimum_free
        self.options = dict(username=username, password=password, connection_timeout=10,
                            connection_cache={}, require_signing=True)

    def prepare(self):
        self.client.makedirs(self.target.directory, exist_ok=True, **self.options)
        self.client.makedirs(self.target.staging, exist_ok=True, **self.options)
        self.check_space()

    def check_space(self):
        volume = self.client.stat_volume(self.target.directory, **self.options)
        if volume.caller_available_size < self.minimum_free + 8 * 1024 * 1024:
            raise DownloadError("SMB free space is below the configured reserve")

    def open(self, path, mode):
        return self.client.open_file(path, mode=mode, share_access="r", **self.options)

    def rename(self, source, destination):
        # rename, not replace: never overwrite an existing library file.
        self.client.rename(source, destination, **self.options)

    def exists(self, path):
        return self.client.path.exists(path, **self.options)

    def stat(self, path):
        return self.client.stat(path, **self.options)

    def checksum(self, path):
        digest = hashlib.sha256()
        with self.open(path, "rb") as file:
            while part := file.read(CHUNK_SIZE):
                digest.update(part)
        return digest.hexdigest()

    def remove_staging(self, path):
        if ntpath.dirname(path) != self.target.staging:
            raise ValueError("Refusing to remove a path outside this download's staging directory")
        try:
            self.client.remove(path, **self.options)
        except FileNotFoundError:
            pass

    def close(self):
        self.client.reset_connection_cache(connection_cache=self.options["connection_cache"])


def metadata_from_track(video_id: str, track) -> dict:
    external = getattr(track, "external_ids", ()) or ()
    return {
        "youtube_id": video_id,
        "title": str(getattr(track, "name", "") or "Unknown"),
        "artist": "; ".join(str(a.name) for a in getattr(track, "artists", ())),
        "album": str(getattr(getattr(track, "album", None), "name", "") or ""),
        "duration": getattr(track, "duration", 0) or 0,
        "track": getattr(track, "track_number", 0) or 0,
        "disc": getattr(track, "disc_number", 0) or 0,
        "external_ids": [[getattr(kind, "value", str(kind)), value] for kind, value in external],
    }


class ListeningTracker:
    """Count advancing player time per queue, excluding seeks, pauses and prefetch."""

    def __init__(self):
        self.queues = {}

    def observe(self, queue, instance_id, now):
        item = queue.current_item
        details = getattr(item, "streamdetails", None)
        playing = getattr(queue.state, "value", queue.state) == "playing"
        if (not playing or not item or not details or details.provider != instance_id
                or getattr(details, "stream_error", False)
                or getattr(item.media_type, "value", item.media_type) != "track"):
            self.queues.pop(queue.queue_id, None)
            return None
        video_id = details.item_id.split("@", 1)[0]
        if not _VIDEO_ID.fullmatch(video_id):
            return None
        position = float(queue.elapsed_time)
        key = (item.queue_item_id, details.item_id)
        previous = self.queues.get(queue.queue_id)
        listened = previous[4] if previous and previous[0] == key else 0
        if previous and previous[0] == key:
            elapsed = now - previous[1]
            advanced = position - previous[2]
            # A position jump is a seek, not time spent listening.
            if 0 < elapsed <= 5 and 0 < advanced <= elapsed + 1:
                listened += min(elapsed, advanced)
        submitted = previous[3] if previous and previous[0] == key else False
        ready = listened >= LISTEN_SECONDS and not submitted
        self.queues[queue.queue_id] = (key, now, position, submitted or ready, listened)
        if ready:
            metadata = metadata_from_track(video_id, item.media_item)
            # A trimmed queue entry has the window's duration, not the full video duration.
            if "@" in details.item_id:
                metadata["duration"] = 0
            return video_id, metadata
        return None


def remux_arguments(metadata: dict, extension: str) -> list[str]:
    args = ["-map", "0:a:0", "-vn", "-c:a", "copy", "-map_metadata", "-1"]
    tags = {k: metadata[k] for k in ("title", "artist", "album", "track", "disc") if metadata.get(k)}
    tags["album_artist"] = metadata.get("artist") or "Unknown Artist"
    tags["comment"] = "https://music.youtube.com/watch?v=" + metadata["youtube_id"]
    for kind, value in metadata.get("external_ids", ()):
        if kind in ("isrc", "musicbrainz_recordingid", "musicbrainz_trackid"):
            tags[kind] = value
    for key, value in tags.items():
        args += ["-metadata", f"{key}={value}"]
    if extension == "m4a":
        args += ["-movflags", "frag_keyframe+empty_moov+default_base_moof", "-f", "mp4"]
    else:
        args += ["-f", "opus"]
    return args + ["pipe:1"]


async def ffmpeg_pipe(source, sink, arguments):
    """Bounded pipes only; FFmpeg never opens a URL or a local media file."""
    process = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror",
        "-protocol_whitelist", "pipe", "-i", "pipe:0", *arguments,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )

    async def feed():
        try:
            async for part in source:
                process.stdin.write(part)
                await process.stdin.drain()
        finally:
            process.stdin.close()
            if hasattr(source, "aclose"):
                await source.aclose()

    async def read():
        while part := await process.stdout.read(CHUNK_SIZE):
            await sink(part)

    async def errors():
        # Never return stderr: libraries may put paths or response headers in it.
        size = 0
        while part := await process.stderr.read(CHUNK_SIZE):
            size += len(part)
            if size >= 256 * 1024:
                raise DownloadError("FFmpeg produced excessive diagnostic output")

    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(feed())
            group.create_task(read())
            group.create_task(errors())
        if await process.wait():
            raise DownloadError("Audio processing or validation failed")
    finally:
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
        await process.wait()


async def validate_audio(store, path, expected_duration, expected_hash=None):
    """Read back and fully decode the remote file before publishing it."""
    file = await blocking(store.open, path, "rb")
    digest = hashlib.sha256()
    progress = bytearray()

    async def source():
        while part := await blocking(file.read, CHUNK_SIZE):
            digest.update(part)
            yield part

    async def collect(part):
        progress.extend(part)
        if len(progress) > 64 * 1024:
            del progress[:-32 * 1024]

    try:
        await ffmpeg_pipe(source(), collect, ["-map", "0:a:0", "-progress", "pipe:1", "-f", "null", "-"])
    finally:
        await blocking(file.close)
    times = re.findall(rb"out_time_us=(\d+)", progress)
    duration = int(times[-1]) / 1_000_000 if times else 0
    if not math.isfinite(duration) or duration <= 0:
        raise DownloadError("Downloaded audio has no valid duration")
    if expected_duration and abs(duration - expected_duration) > max(3, expected_duration * 0.03):
        raise DownloadError("Downloaded audio duration differs from the expected track")
    if expected_hash and digest.hexdigest() != expected_hash:
        raise DownloadError("SMB read-back checksum does not match the uploaded audio")
    return duration


class DownloadManager:
    def __init__(self, provider, target, username, password, minimum_free):
        self.provider = provider
        key = hashlib.sha256((provider.instance_id + target.identity).encode()).hexdigest()[:24]
        self.queue = DownloadQueue(Path(provider.mass.storage_path) / "ytmusic_free_downloads" / f"{key}.sqlite3")
        self.store = SMBStore(target, username, password, minimum_free)
        self.wake = asyncio.Event()
        self.stopping = False
        self.submissions = set()
        self.submitting = set()
        self.worker = None
        self.monitor = None
        from .smb_cache import PlaybackCache
        self.playback = PlaybackCache(self)

    async def start(self):
        await blocking(self.queue.initialize)
        self.worker = asyncio.create_task(self.run())
        self.monitor = asyncio.create_task(self.observe_playback())

    async def observe_playback(self):
        tracker = ListeningTracker()
        while not self.stopping:
            try:
                queues = self.provider.mass.player_queues.all()
                tracker.queues = {key: value for key, value in tracker.queues.items()
                                  if key in {queue.queue_id for queue in queues}}
                for queue in queues:
                    if ready := tracker.observe(queue, self.provider.instance_id, time.monotonic()):
                        self.submit(*ready)
            except Exception as err:
                self.provider.logger.warning("Playback download observer failed (%s)", type(err).__name__)
            await asyncio.sleep(2)

    def submit(self, video_id, metadata):
        if self.stopping or video_id in self.submitting:
            return
        self.submitting.add(video_id)

        async def enqueue():
            try:
                if await blocking(self.queue.enqueue, video_id, metadata):
                    self.wake.set()
            except Exception as err:
                self.provider.logger.warning("Could not queue download %s (%s)", video_id, type(err).__name__)
            finally:
                self.submitting.discard(video_id)

        task = asyncio.create_task(enqueue())
        self.submissions.add(task)
        task.add_done_callback(self.submissions.discard)

    async def run(self):
        while not self.stopping:
            self.wake.clear()
            try:
                async with _DOWNLOAD_SLOT:
                    job = await blocking(self.queue.claim, time.time())
                    if job:
                        await self.process(job)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self.provider.logger.warning("Download queue unavailable (%s)", type(err).__name__)
                job = None
            if not job:
                with suppress(TimeoutError):
                    await asyncio.wait_for(self.wake.wait(), 30)

    async def process(self, job):
        video_id, metadata, attempt = job
        try:
            async with asyncio.timeout(1800):
                await self.download(video_id, metadata)
            await blocking(self.queue.finish, video_id)
            self.provider.logger.info("SMB download complete: %s", video_id)
        except asyncio.CancelledError:
            await blocking(self.queue.interrupted, video_id)
            raise
        except Exception as err:
            # Exception text can contain passwords, cookies or signed URLs.
            reason = str(err) if isinstance(err, DownloadError) else type(err).__name__
            await blocking(self.queue.fail, video_id, attempt, reason)
            self.provider.logger.warning("SMB download %s failed, attempt %s/%s (%s)",
                                         video_id, attempt, MAX_ATTEMPTS, reason)

    async def download(self, video_id, metadata):
        from music_assistant_models.enums import MediaType

        store = self.store
        # Reusing an existing validated copy requires neither free space nor write access.
        for extension in ("opus", "m4a"):
            existing = ntpath.join(store.target.directory, video_id + "." + extension)
            if await blocking(store.exists, existing):
                await self.index_existing(video_id, existing, metadata, extension)
                return
        await blocking(store.prepare)
        details = await self.provider._get_online_stream_details(video_id, MediaType.TRACK)
        codec = getattr(details.audio_format.content_type, "value", "")
        extension = "opus" if codec in ("opus", "webm") else "m4a" if codec in ("m4a", "mp4", "mp4a", "aac") else None
        if extension is None:
            raise DownloadError("The selected audio codec cannot be saved without transcoding")
        temporary = ntpath.join(store.target.staging, video_id + ".part." + extension)
        final = ntpath.join(store.target.directory, video_id + "." + extension)
        output = None
        size = 0
        digest = hashlib.sha256()
        try:
            output = await blocking(store.open, temporary, "wb")

            async def write(part):
                nonlocal size
                size += len(part)
                if size > MAX_AUDIO_BYTES:
                    raise DownloadError("Audio exceeds the 256 MiB per-track limit")
                if size // (8 * 1024 * 1024) != (size - len(part)) // (8 * 1024 * 1024):
                    await blocking(store.check_space)
                await blocking(output.write, part)
                digest.update(part)

            await ffmpeg_pipe(self.provider.get_audio_stream(details), write, remux_arguments(metadata, extension))
            await blocking(output.close)
            output = None
            duration = await validate_audio(store, temporary, metadata.get("duration"), digest.hexdigest())
            saved = {**metadata, "duration": duration, "sha256": digest.hexdigest(), "bytes": size,
                     "codec": codec, "sample_rate": details.audio_format.sample_rate}
            # Atomic audio rename is the publication point. A collision leaves the old file intact.
            await blocking(store.rename, temporary, final)
            stat = await blocking(store.stat, final)
            await blocking(self.queue.remember, video_id, ntpath.basename(final), saved, stat)
        finally:
            if output:
                with suppress(Exception):
                    await blocking(output.close)
            with suppress(Exception):
                await blocking(store.remove_staging, temporary)

    async def index_existing(self, video_id, path, metadata, extension):
        """Validate an existing audio file by exact ID, using SQLite metadata only."""
        store = self.store
        before = await blocking(store.stat, path)
        saved = dict(metadata)
        checksum = await blocking(store.checksum, path)
        if saved.get("sha256") and saved["sha256"] != checksum:
            raise DownloadError("Existing audio checksum differs from its recorded download")
        duration = await validate_audio(store, path, saved.get("duration"), checksum)
        after = await blocking(store.stat, path)
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise DownloadError("Existing audio changed during validation")
        saved.update(youtube_id=video_id, sha256=checksum, bytes=after.st_size,
                     codec="opus" if extension == "opus" else "m4a", duration=duration)
        await blocking(self.queue.remember, video_id, ntpath.basename(path), saved, after)

    async def stop(self):
        self.stopping = True
        await self.playback.stop()
        if self.monitor:
            self.monitor.cancel()
            with suppress(asyncio.CancelledError):
                await self.monitor
        for task in list(self.submissions):
            task.cancel()
        if self.submissions:
            await asyncio.gather(*list(self.submissions), return_exceptions=True)
        if self.worker:
            self.worker.cancel()
            with suppress(asyncio.CancelledError):
                await self.worker
        try:
            await blocking(self.store.close)
        except Exception as err:
            self.provider.logger.warning("SMB session cleanup failed (%s)", type(err).__name__)
