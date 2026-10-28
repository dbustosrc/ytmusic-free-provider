"""Indexed playback, process deadlines, safe migration and time-based fallback."""

import asyncio
import errno
from contextlib import aclosing
import hashlib
import io
import json
import ntpath
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

import ytmusic_free as ytm
from ytmusic_free import smb_cache as cache
from ytmusic_free.cache_reader import read
from ytmusic_free import downloads as dl
from test_downloads import VIDEO, MemoryStore, manager_for_test


def entry(data=b"audio", *, mtime=123):
    return {"video_id": VIDEO, "filename": VIDEO + ".opus", "size": len(data), "mtime_ns": mtime,
            "metadata": {"youtube_id": VIDEO, "sha256": hashlib.sha256(data).hexdigest(), "duration": 3}}


def indexed(queue, data=b"audio"):
    e = entry(data)
    queue.enqueue(VIDEO, e["metadata"])
    queue.remember(VIDEO, e["filename"], e["metadata"], NS(st_size=e["size"], st_mtime_ns=e["mtime_ns"]))
    queue.finish(VIDEO)
    return e


def test_index_survives_restart_and_deleted_audio_is_requeued_only_after_listening(tmp_path):
    q = dl.DownloadQueue(tmp_path / "queue.sqlite3")
    q.initialize()
    expected = indexed(q)
    q.initialize()
    assert q.cached(VIDEO) == expected
    assert q.claim(float("inf")) is None
    q.unavailable(VIDEO, missing=True)
    assert q.cached(VIDEO) is None
    assert q.claim(float("inf")) is None
    assert q.enqueue(VIDEO, {"youtube_id": VIDEO})
    assert q.claim(float("inf"))[2] == 1


def test_legacy_done_jobs_migrate_once_in_background(tmp_path):
    q = dl.DownloadQueue(tmp_path / "queue.sqlite3")
    q.initialize()
    q.enqueue(VIDEO, {})
    q.finish(VIDEO)
    q.initialize()
    assert q.claim(float("inf"))[0] == VIDEO


def test_existing_json_is_never_read_or_modified(monkeypatch, tmp_path):
    store = MemoryStore()
    path = ntpath.join(store.target.directory, VIDEO + ".opus")
    store.files[path] = b"audio"
    sidecar = ntpath.join(store.target.directory, VIDEO + ".json")
    original = json.dumps({"youtube_id": VIDEO, "sha256": hashlib.sha256(b"audio").hexdigest(),
                           "external_ids": [["isrc", "TEST123"]]}).encode()
    store.files[sidecar] = original
    manager = manager_for_test(store, tmp_path)
    original_open = store.open
    def audio_only(path, mode):
        assert not path.endswith(".json"), "JSON sidecars must never be read by the provider"
        return original_open(path, mode)
    monkeypatch.setattr(store, "open", audio_only)
    monkeypatch.setattr(dl, "validate_audio", AsyncMock(return_value=3))
    asyncio.run(manager.download(VIDEO, {"youtube_id": VIDEO}))
    assert "external_ids" not in manager.queue.cached(VIDEO)["metadata"]
    assert store.files[sidecar] == original


def test_sqlite_checksum_mismatch_is_not_indexed(monkeypatch, tmp_path):
    store = MemoryStore()
    store.files[ntpath.join(store.target.directory, VIDEO + ".opus")] = b"changed"
    store.files[ntpath.join(store.target.directory, VIDEO + ".json")] = json.dumps(
        {"youtube_id": VIDEO, "sha256": "0" * 64}).encode()
    manager = manager_for_test(store, tmp_path)
    with pytest.raises(dl.DownloadError, match="checksum"):
        asyncio.run(manager.download(VIDEO, {"youtube_id": VIDEO, "sha256": "0" * 64}))
    assert manager.queue.cached(VIDEO) is None


def test_reader_checks_exact_path_without_scanning_and_streams_without_hash_pass():
    e = entry()
    client = Mock()
    client.stat.return_value = NS(st_size=5, st_mtime_ns=123)
    file = io.BytesIO(b"audio")
    client.open_file.return_value = file
    output = io.BytesIO()
    read({"entry": e, "directory": r"\\nas\share\Music", "username": "user", "password": "secret"}, client, output)
    header, audio = output.getvalue().split(b"\n", 1)
    assert json.loads(header)["state"] == "ready"
    assert audio == b"audio"
    client.listdir.assert_not_called()
    client.stat.assert_called_once()
    assert client.open_file.call_args.args[0] == r"\\nas\share\Music\abcdefghijk.opus"


@pytest.mark.parametrize("state", ["missing", "invalid", "changed-valid", "changed-corrupt"])
def test_reader_missing_corruption_and_revalidation(state):
    client = Mock()
    if state == "missing":
        client.stat.side_effect = FileNotFoundError()
    else:
        client.stat.return_value = NS(st_size=7 if state == "invalid" else 5,
                                     st_mtime_ns=456 if state.startswith("changed") else 123)
    client.open_file.return_value = io.BytesIO(b"other" if state == "changed-corrupt" else b"audio")
    output = io.BytesIO()
    read({"entry": entry(), "directory": r"\\nas\share\Music", "username": "u", "password": "p"}, client, output)
    header = json.loads(output.getvalue().split(b"\n", 1)[0])
    assert header["state"] == {"changed-valid": "ready", "changed-corrupt": "invalid"}.get(state, state)


def test_reader_rejects_path_traversal():
    e = entry()
    e["filename"] = "../keep.opus"
    with pytest.raises(ValueError):
        read({"entry": e}, Mock(), io.BytesIO())


def test_smb_oserror_missing_is_distinct_from_permission_denied():
    client = Mock()
    config = {"entry": entry(), "directory": r"\\nas\share\Music", "username": "u", "password": "p"}
    client.stat.side_effect = OSError(errno.ENOENT, "missing")
    output = io.BytesIO()
    read(config, client, output)
    assert json.loads(output.getvalue())["state"] == "missing"
    client.stat.side_effect = OSError(errno.EACCES, "permission")
    with pytest.raises(OSError):
        read(config, client, io.BytesIO())


def test_cache_hit_skips_youtube_and_preserves_trim(provider):
    provider._downloads = NS(playback=NS(lookup=AsyncMock(return_value=entry())))
    provider.config = NS(get_value=lambda key: True)
    provider._get_online_stream_details = AsyncMock(side_effect=AssertionError("YouTube must not be contacted"))
    details = asyncio.run(provider.get_stream_details(VIDEO + "@1-2", ytm.MediaType.TRACK))
    assert details.data["cache_entry"]["video_id"] == VIDEO
    assert details.audio_format.content_type.value == "f32le"
    assert details.duration == 1
    assert details.extra_input_args == ["-ss", "1", "-t", "1"]
    provider._get_online_stream_details.assert_not_called()


def test_disabled_preference_and_misses_keep_original_online_behavior(provider):
    provider._downloads = NS(playback=NS(lookup=AsyncMock(return_value=None)))
    provider.config = NS(get_value=lambda key: False)
    provider._get_online_stream_details = AsyncMock(return_value="online")
    assert asyncio.run(provider.get_stream_details(VIDEO, ytm.MediaType.TRACK)) == "online"
    provider._downloads.playback.lookup.assert_not_called()
    provider.config = NS(get_value=lambda key: True)
    assert asyncio.run(provider.get_stream_details(VIDEO, ytm.MediaType.TRACK)) == "online"


def test_lookup_only_reads_sqlite_and_cooldown_skips_io(tmp_path):
    manager = manager_for_test(MemoryStore(), tmp_path)
    manager.stopping = False
    playback = cache.PlaybackCache(manager)
    indexed(manager.queue)
    assert asyncio.run(playback.lookup(VIDEO))
    playback.failed(TimeoutError("secret"))
    assert asyncio.run(playback.lookup(VIDEO)) is None


def test_timeout_kills_smb_process_without_waiting_for_library(monkeypatch, tmp_path):
    async def run():
        manager = manager_for_test(MemoryStore(), tmp_path)
        manager.store.options = {"username": "synthetic", "password": "secret"}
        playback = cache.PlaybackCache(manager)
        original = asyncio.create_subprocess_exec
        processes = []

        async def hanging(*args, **kwargs):
            process = await original(sys.executable, "-c", "import time; time.sleep(60)", **kwargs)
            processes.append(process)
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", hanging)
        monkeypatch.setattr(cache, "START_TIMEOUT", .1)
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            async for part in playback.remote(entry()):
                pass
        assert time.monotonic() - start < 3
        assert processes[0].returncode is not None
        assert not playback.processes

    asyncio.run(run())


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="FFmpeg required")
def test_midstream_failure_resumes_online_by_pcm_frame_without_splicing(monkeypatch, tmp_path):
    audio = subprocess.check_output(["ffmpeg", "-v", "error", "-f", "lavfi", "-i",
        "sine=frequency=440:duration=12", "-c:a", "libopus", "-f", "webm", "pipe:1"])

    async def source():
        for pos in range(0, len(audio), 1024):
            yield audio[pos:pos + 1024]

    async def collect(gen):
        async with aclosing(gen):
            return b"".join([part async for part in gen])

    async def run():
        baseline = await collect(cache.decode(source()))
        cutoff = 48000 * 8 * 4
        manager = manager_for_test(MemoryStore(), tmp_path)
        manager.provider._get_online_stream_details.return_value = "online-details"

        async def online(details):
            assert details == "online-details"
            async for part in source():
                yield part

        manager.provider._get_youtube_audio_stream = online
        playback = cache.PlaybackCache(manager)

        async def broken(entry):
            yield b"cached placeholder"

        original = cache.decode
        calls = []

        async def controlled(src, *, skip_frames=0):
            if not calls:
                calls.append("cached")
                async with aclosing(src):
                    yield baseline[:cutoff]
                    raise OSError("NAS disconnected")
            calls.append(skip_frames)
            async for part in original(src, skip_frames=skip_frames):
                yield part

        monkeypatch.setattr(playback, "remote", broken)
        monkeypatch.setattr(cache, "decode", controlled)
        result = await collect(playback.stream(entry()))
        assert result == baseline
        assert calls == ["cached", cutoff // cache.PCM_FRAME_BYTES]
        assert time.monotonic() < playback.retry_at

    asyncio.run(run())


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="FFmpeg required")
def test_real_decoder_recovers_a_truncated_cached_source(monkeypatch, tmp_path):
    audio = subprocess.check_output(["ffmpeg", "-v", "error", "-f", "lavfi", "-i",
        "sine=frequency=440:duration=30", "-c:a", "libopus", "-f", "webm", "pipe:1"])

    async def source():
        yield audio

    async def run():
        async with aclosing(cache.decode(source())) as stream:
            baseline = b"".join([p async for p in stream])
        manager = manager_for_test(MemoryStore(), tmp_path)
        manager.provider._get_youtube_audio_stream = lambda details: source()
        playback = cache.PlaybackCache(manager)

        async def broken(entry):
            yield audio[:len(audio) // 2]
            raise OSError("SMB link lost")

        monkeypatch.setattr(playback, "remote", broken)
        original = cache.decode
        skips = []

        async def observe(src, *, skip_frames=0):
            skips.append(skip_frames)
            async with aclosing(original(src, skip_frames=skip_frames)) as decoded:
                async for part in decoded:
                    yield part

        monkeypatch.setattr(cache, "decode", observe)
        async with aclosing(playback.stream(entry())) as stream:
            actual = b"".join([p async for p in stream])
        assert len(skips) == 2 and skips[1] > 0
        assert actual == baseline

    asyncio.run(run())


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="FFmpeg required")
def test_decoder_cancellation_closes_a_partially_consumed_stream():
    audio = subprocess.check_output(["ffmpeg", "-v", "error", "-f", "lavfi", "-i",
        "sine=frequency=440:duration=30", "-c:a", "libopus", "-f", "webm", "pipe:1"])

    async def run():
        closed = asyncio.Event()
        async def source():
            try:
                yield audio
                await asyncio.sleep(60)
            finally:
                closed.set()
        stream = cache.decode(source())
        assert await anext(stream)
        await asyncio.wait_for(stream.aclose(), 3)
        assert closed.is_set()

    asyncio.run(run())
