"""Offline queue, isolation, publication and listening regressions."""

import asyncio
import io
import json
import logging
import ntpath
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

import ytmusic_free as ytm
from ytmusic_free import downloads as dl

VIDEO = "abcdefghijk"


@pytest.mark.parametrize("value", ["smb://nas/share/Music", "//nas/share/Music", r"\\nas\share\Music"])
def test_remote_directory_and_staging_are_separate(value):
    target = dl.SMBTarget.parse(value)
    assert target.directory == r"\\nas\share\Music"
    assert target.staging.startswith(r"\\nas\share\.ytmusic-free-staging" + "\\")
    assert not target.staging.startswith(target.directory + "\\")
    assert target.identity == dl.SMBTarget.parse("smb://NAS/share/music").identity


@pytest.mark.parametrize("value", ["/data/music", "C:/music", "smb://nas/share", "smb://u:secret@nas/share/Music",
                                     "smb://nas/share/../Music", "smb://nas/share/%2e%2e/Music",
                                     "smb://nas/share/.ytmusic-free-staging/music", "smb://nas/share/Music?x=1",
                                     "smb://nas/share//Music", "smb://nas/share/Music%00"])
def test_unsafe_or_local_directories_are_rejected(value):
    with pytest.raises(ValueError):
        dl.SMBTarget.parse(value)


def test_queue_survives_restart_deduplicates_and_limits_retries(tmp_path):
    path = tmp_path / "state.sqlite3"
    queue = dl.DownloadQueue(path)
    queue.initialize()
    assert queue.enqueue(VIDEO, {"youtube_id": VIDEO, "title": "Track"})
    assert not queue.enqueue(VIDEO, {"title": "changed"})
    job = queue.claim(float("inf"))
    assert job[2] == 1
    restarted = dl.DownloadQueue(path)
    restarted.initialize()
    assert restarted.claim(float("inf"))[2] == 2
    restarted.fail(VIDEO, 2, "network")
    assert restarted.claim(float("inf"))[2] == 3
    restarted.fail(VIDEO, 3, "network")
    assert restarted.claim(float("inf")) is None
    with restarted.connect() as db:
        assert db.execute("SELECT status FROM jobs").fetchone() == ("failed",)
    assert not restarted.enqueue(VIDEO, {})


def test_cancelled_job_keeps_its_attempt_budget(tmp_path):
    queue = dl.DownloadQueue(tmp_path / "state.sqlite3")
    queue.initialize()
    queue.enqueue(VIDEO, {})
    assert queue.claim(float("inf"))[2] == 1
    queue.interrupted(VIDEO)
    assert queue.claim(float("inf"))[2] == 1
    queue.finish(VIDEO)
    assert queue.claim(float("inf")) is None
    with pytest.raises(ValueError):
        queue.enqueue("../../escape", {})


def player(position=0, *, state="playing", item="item1", provider="source"):
    track = NS(name="Song", artists=[NS(name="Artist")], album=NS(name="Album"), duration=90)
    current = NS(queue_item_id=item, media_item=track, media_type="track",
                 streamdetails=NS(provider=provider, item_id=VIDEO, stream_error=False))
    return NS(queue_id="player1", state=state, elapsed_time=position, current_item=current)


def test_only_actual_advancing_listening_submits_once():
    tracker = dl.ListeningTracker()
    results = [tracker.observe(player(t), "source", t) for t in range(0, 35, 2)]
    ready = [r for r in results if r]
    assert len(ready) == 1
    assert ready[0][0] == VIDEO
    assert ready[0][1]["album"] == "Album"


def test_seek_stall_pause_other_provider_and_prefetch_do_not_count():
    tracker = dl.ListeningTracker()
    assert tracker.observe(player(0), "source", 0) is None
    assert tracker.observe(player(120), "source", 2) is None
    assert tracker.observe(player(120), "source", 4) is None
    assert tracker.observe(player(120, state="paused"), "source", 80) is None
    assert tracker.observe(player(120), "source", 82) is None
    assert tracker.observe(player(121), "source", 84) is None
    assert tracker.observe(player(160, provider="other"), "source", 120) is None
    assert tracker.observe(player(0, state="idle"), "source", 122) is None
    assert tracker.observe(player(130, item="different"), "source", 124) is None


def test_disabled_downloads_do_not_create_state_or_connect(provider, monkeypatch):
    provider.config = Mock()
    provider.config.get_value.return_value = False
    constructor = Mock(side_effect=AssertionError("must remain unused"))
    monkeypatch.setattr(dl, "DownloadManager", constructor)
    asyncio.run(provider._start_downloads())
    assert provider._downloads is None
    constructor.assert_not_called()


def test_bad_download_settings_do_not_disable_the_provider(provider):
    values = {ytm.CONF_DOWNLOAD_ENABLED: True, ytm.CONF_DOWNLOAD_SMB: "/local"}
    provider.config = Mock()
    provider.config.get_value.side_effect = values.get
    asyncio.run(provider._start_downloads())
    assert provider._downloads is None


def test_settings_are_opt_in_conditional_and_password_is_secret():
    entries = {entry.key: entry for entry in ytm._build_config_entries()}
    assert entries[ytm.CONF_DOWNLOAD_ENABLED].default_value is False
    assert entries[ytm.CONF_DOWNLOAD_PASSWORD].type.value == "secure_string"
    for key in (ytm.CONF_DOWNLOAD_SMB, ytm.CONF_DOWNLOAD_USER, ytm.CONF_DOWNLOAD_PASSWORD,
                ytm.CONF_DOWNLOAD_FREE_GIB):
        assert entries[key].depends_on == ytm.CONF_DOWNLOAD_ENABLED
        assert entries[key].depends_on_value is True


def test_remote_sessions_are_isolated_and_require_signing():
    a = dl.SMBStore(dl.SMBTarget.parse("smb://nas/share/Music"), "one", "secret1", 1024)
    b = dl.SMBStore(dl.SMBTarget.parse("smb://nas/share/Music"), "two", "secret2", 1024)
    assert a.options["connection_cache"] is not b.options["connection_cache"]
    assert a.options["require_signing"] is True
    with pytest.raises(ValueError):
        a.remove_staging(r"\\nas\share\Music\keep.opus")


def test_low_remote_space_is_a_controlled_error():
    store = dl.SMBStore(dl.SMBTarget.parse("smb://nas/share/Music"), "one", "secret", 1024 ** 3)
    store.client = Mock()
    store.client.stat_volume.return_value = NS(caller_available_size=10)
    with pytest.raises(dl.DownloadError, match="free space"):
        store.check_space()


def test_remux_preserves_codec_and_metadata_without_shell():
    data = {"youtube_id": VIDEO, "title": "Song $(secret)", "artist": "Artist", "album": "Album"}
    for extension in ("opus", "m4a"):
        args = dl.remux_arguments(data, extension)
        assert args[args.index("-c:a") + 1] == "copy"
        assert "title=Song $(secret)" in args
        assert "album_artist=Artist" in args
        assert args[-1] == "pipe:1"


class MemoryStore:
    def __init__(self):
        self.target = dl.SMBTarget.parse("smb://nas/share/Music")
        self.files = {}
        self.renames = []

    def prepare(self):
        pass

    def check_space(self):
        pass

    def exists(self, path):
        return path in self.files

    def open(self, path, mode):
        store = self

        class File(io.BytesIO):
            def close(self):
                if not self.closed and "w" in mode:
                    store.files[path] = self.getvalue()
                super().close()

        return File(self.files.get(path, b"") if "r" in mode else b"")

    def rename(self, old, new):
        assert new not in self.files
        self.renames.append((old, new))
        self.files[new] = self.files.pop(old)

    def remove_staging(self, path):
        self.files.pop(path, None)

    def stat(self, path):
        if path not in self.files:
            raise FileNotFoundError()
        return NS(st_size=len(self.files[path]), st_mtime_ns=123)

    def checksum(self, path):
        import hashlib
        return hashlib.sha256(self.files[path]).hexdigest()


def manager_for_test(store, tmp_path):
    manager = object.__new__(dl.DownloadManager)
    manager.stopping = False
    manager.store = store
    manager.provider = NS(logger=logging.getLogger("downloads_test"),
        _get_online_stream_details=AsyncMock(return_value=NS(audio_format=NS(content_type=NS(value="opus"), sample_rate=48000))))

    async def audio(details):
        yield b"source"

    manager.provider.get_audio_stream = audio
    manager.queue = dl.DownloadQueue(tmp_path / "state.sqlite3")
    manager.queue.initialize()
    return manager


def test_publish_only_after_validation_and_keep_metadata(monkeypatch, tmp_path):
    store = MemoryStore()
    manager = manager_for_test(store, tmp_path)

    async def remux(source, sink, args):
        async for part in source:
            await sink(b"saved:" + part)

    async def validate(store, path, duration, expected_hash=None):
        assert not store.renames
        assert path in store.files
        return 90

    monkeypatch.setattr(dl, "ffmpeg_pipe", remux)
    monkeypatch.setattr(dl, "validate_audio", validate)
    asyncio.run(manager.download(VIDEO, {"youtube_id": VIDEO, "title": "Song", "duration": 90}))
    final = ntpath.join(store.target.directory, VIDEO + ".opus")
    assert store.files[final] == b"saved:source"
    metadata = manager.queue.cached(VIDEO)["metadata"]
    assert metadata["youtube_id"] == VIDEO
    assert metadata["sha256"]
    assert not any(path.endswith(".json") for path in store.files)
    assert not any(path.startswith(store.target.staging) for path in store.files)


def test_invalid_download_is_not_published_and_failure_hides_secrets(monkeypatch, tmp_path, caplog):
    store = MemoryStore()
    manager = manager_for_test(store, tmp_path)

    async def fail(source, sink, args):
        await sink(b"partial")
        raise OSError("Cookie=secret SMB=secret https://signed.invalid/?token=secret")

    monkeypatch.setattr(dl, "ffmpeg_pipe", fail)
    manager.queue.enqueue(VIDEO, {"youtube_id": VIDEO})
    with caplog.at_level(logging.WARNING):
        asyncio.run(manager.process(manager.queue.claim(float("inf"))))
    assert not store.files
    assert not store.renames
    assert "secret" not in caplog.text
    with manager.queue.connect() as db:
        status, error = db.execute("SELECT status,error FROM jobs").fetchone()
        assert status == "pending" and error == "OSError"


def test_replay_of_existing_download_does_not_fetch_again(monkeypatch, tmp_path):
    store = MemoryStore()
    store.files[ntpath.join(store.target.directory, VIDEO + ".opus")] = b"existing"
    manager = manager_for_test(store, tmp_path)
    monkeypatch.setattr(dl, "validate_audio", AsyncMock(return_value=90))
    asyncio.run(manager.download(VIDEO, {"duration": 90}))
    manager.provider._get_online_stream_details.assert_not_called()
    assert not store.renames


@pytest.mark.parametrize("fails", [False, True])
def test_cancellation_finishes_io_before_handle_cleanup(fails):
    import threading

    async def run():
        started, finish = threading.Event(), threading.Event()
        unhandled = []
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: unhandled.append(context))

        def operation():
            started.set()
            finish.wait(2)
            if fails:
                raise OSError("synthetic secret must not reach the event loop handler")
            return True

        task = asyncio.create_task(dl.blocking(operation))
        while not started.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        assert not unhandled

    asyncio.run(run())


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="FFmpeg is required for real media checks")
@pytest.mark.parametrize("extension,codec,container", [("opus", "libopus", "webm"), ("m4a", "aac", "mp4")])
def test_real_ffmpeg_stream_copy_tags_and_full_validation(extension, codec, container, tmp_path):
    args = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
            "sine=frequency=440:duration=3", "-c:a", codec]
    if container == "mp4":
        args += ["-movflags", "frag_keyframe+empty_moov+default_base_moof"]
    audio = subprocess.check_output(args + ["-f", container, "pipe:1"])
    store = MemoryStore()
    manager = manager_for_test(store, tmp_path)
    manager.provider._get_online_stream_details.return_value.audio_format.content_type.value = extension

    async def source(details):
        for pos in range(0, len(audio), 512):
            yield audio[pos:pos + 512]

    manager.provider.get_audio_stream = source
    metadata = {"youtube_id": VIDEO, "title": "Tést Song", "artist": "An Artist",
                "album": "An Album", "duration": 3}
    asyncio.run(manager.download(VIDEO, metadata))
    saved = store.files[ntpath.join(store.target.directory, VIDEO + "." + extension)]
    probe = subprocess.run(["ffprobe", "-v", "error", "-i", "pipe:0", "-show_streams", "-show_format",
                            "-of", "json"], input=saved, capture_output=True, check=True)
    info = json.loads(probe.stdout)
    assert info["streams"][0]["codec_name"] == ("opus" if extension == "opus" else "aac")
    tags = {k.lower(): v for section in [*info["streams"], info["format"]] for k, v in section.get("tags", {}).items()}
    assert tags["title"] == "Tést Song"
    assert tags["album"] == "An Album"
    assert tags["artist"] == "An Artist"
    assert not any(path.startswith(store.target.staging) for path in store.files)


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="FFmpeg is required for real media checks")
def test_real_ffmpeg_rejects_corrupt_audio_without_publication(tmp_path):
    store = MemoryStore()
    manager = manager_for_test(store, tmp_path)

    async def source(details):
        yield b"not audio"

    manager.provider.get_audio_stream = source
    with pytest.raises((dl.DownloadError, ExceptionGroup)):
        asyncio.run(manager.download(VIDEO, {"youtube_id": VIDEO, "duration": 90}))
    assert not store.renames
    assert not store.files
