"""Indexed SMB playback with bounded failures and sample-position fallback."""

import asyncio
from contextlib import aclosing, suppress
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace

from .downloads import blocking

START_TIMEOUT = 2.0
READ_TIMEOUT = 3.0
COOLDOWN_SECONDS = 60
PCM_RATE = 48000
PCM_CHANNELS = 2
PCM_FRAME_BYTES = 4 * PCM_CHANNELS
PCM_BYTES_PER_SECOND = PCM_RATE * PCM_FRAME_BYTES


async def kill(process):
    if process.returncode is None:
        with suppress(ProcessLookupError):
            process.kill()
    if process.stdin is not None:
        # Discard queued input when the consumer exits early; close() would try to flush it.
        if not process.stdin.is_closing() or process.stdin.transport.get_write_buffer_size():
            process.stdin.transport.abort()
        with suppress(BrokenPipeError, ConnectionResetError):
            await process.stdin.wait_closed()
    # A killed process can still have a full stdout pipe; drain it before waiting for exit.
    if process.stdout is not None:
        await process.stdout.read()
    await process.wait()


async def decode(source, *, skip_frames=0):
    """Decode either source into the same float PCM without storing or re-encoding it."""
    process = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-xerror",
        "-protocol_whitelist", "pipe", "-i", "pipe:0", "-map", "0:a:0", "-vn",
        "-ar", str(PCM_RATE), "-ac", str(PCM_CHANNELS), "-c:a", "pcm_f32le", "-f", "f32le", "pipe:1",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )

    async def feed():
        async with aclosing(source):
            try:
                async for part in source:
                    process.stdin.write(part)
                    await process.stdin.drain()
            finally:
                process.stdin.close()

    task = asyncio.create_task(feed())
    pending = b""
    skip = skip_frames * PCM_FRAME_BYTES
    try:
        while part := await process.stdout.read(65536):
            pending += part
            length = len(pending) // PCM_FRAME_BYTES * PCM_FRAME_BYTES
            chunk, pending = pending[:length], pending[length:]
            if skip:
                consumed = min(skip, len(chunk))
                skip -= consumed
                chunk = chunk[consumed:]
            if chunk:
                yield chunk
        await task
        if await process.wait() or pending:
            raise OSError("Audio decoding failed")
    finally:
        task.cancel()
        await kill(process)
        with suppress(asyncio.CancelledError, Exception):
            await task


class PlaybackCache:
    def __init__(self, manager):
        self.manager = manager
        self.retry_at = 0.0
        self.processes = set()
        self.readers = set()

    async def lookup(self, video_id):
        if time.monotonic() < self.retry_at or self.manager.stopping:
            return None
        try:
            # SQLite primary-key lookup only: no SMB request and no directory listing.
            return await blocking(self.manager.queue.cached, video_id)
        except Exception as err:
            self.failed(err)
            return None

    def failed(self, err):
        if not isinstance(err, (FileNotFoundError, ValueError)):
            self.retry_at = time.monotonic() + COOLDOWN_SECONDS
        self.manager.provider.logger.warning("SMB cache unavailable (%s); using YouTube", type(err).__name__)

    async def remote(self, entry):
        store = self.manager.store
        config = {"directory": store.target.directory, "username": store.options["username"],
                  "password": store.options["password"], "entry": entry}
        process = await asyncio.create_subprocess_exec(
            sys.executable, str(Path(__file__).with_name("cache_reader.py")),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        self.processes.add(process)
        reader_task = asyncio.current_task()
        self.readers.add(reader_task)
        try:
            process.stdin.write(json.dumps(config).encode())
            await process.stdin.drain()
            process.stdin.close()
            raw = await asyncio.wait_for(process.stdout.readline(), START_TIMEOUT)
            if not raw:
                raise OSError("SMB cache reader could not open the file")
            try:
                header = json.loads(raw)
            except ValueError:
                raise OSError("SMB cache reader returned an invalid response") from None
            if header["state"] in ("missing", "invalid"):
                await blocking(self.manager.queue.unavailable, entry["video_id"], header["state"] == "missing")
                raise FileNotFoundError() if header["state"] == "missing" else ValueError("Invalid cache")
            if header["state"] != "ready" or header["size"] != entry["size"]:
                raise ValueError("Invalid cache reader response")
            if header["mtime_ns"] != entry["mtime_ns"]:
                await blocking(self.manager.queue.remember, entry["video_id"], entry["filename"],
                               entry["metadata"], SimpleNamespace(st_size=header["size"], st_mtime_ns=header["mtime_ns"]))
            while part := await asyncio.wait_for(process.stdout.read(65536), READ_TIMEOUT):
                yield part
            if await process.wait():
                raise OSError("SMB cache stream failed")
        finally:
            await kill(process)
            self.processes.discard(process)
            self.readers.discard(reader_task)

    async def stream(self, entry):
        emitted = 0
        try:
            if time.monotonic() < self.retry_at:
                raise OSError("SMB cache is cooling down")
            async with aclosing(decode(self.remote(entry))) as pcm:
                async for part in pcm:
                    emitted += len(part)
                    yield part
            expected = entry["metadata"].get("duration", 0)
            if expected and abs(emitted / PCM_BYTES_PER_SECOND - expected) > max(3, expected * .03):
                raise ValueError("Cached audio ended prematurely")
            return
        except asyncio.CancelledError:
            raise
        except Exception as err:
            self.failed(err)
        if self.manager.stopping:
            raise asyncio.CancelledError()
        # Discard decoded samples to the last emitted frame. Encoded bytes are never spliced.
        provider = self.manager.provider
        from music_assistant_models.enums import MediaType
        details = await provider._get_online_stream_details(entry["video_id"], MediaType.TRACK)
        async with aclosing(decode(provider._get_youtube_audio_stream(details),
                                   skip_frames=emitted // PCM_FRAME_BYTES)) as pcm:
            async for part in pcm:
                yield part

    async def stop(self):
        tasks = list(self.readers)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
