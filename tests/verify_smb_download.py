"""Run in the candidate image against an isolated Samba fixture, never a real library.

The container must have a tmpfs at /verification and share the fixture's network
namespace. The fixed credentials belong only to that disposable test server.
"""

import asyncio
import hashlib
import json
import logging
import ntpath
from pathlib import Path
import subprocess
from types import SimpleNamespace as NS

from music_assistant.providers.ytmusic_free import downloads as dl

IDS = ("synthetic01", "synthetic02", "synthetic03", "synthetic04")


def tone(codec, container, duration=3):
    args = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
            f"sine=frequency=440:duration={duration}", "-c:a", codec]
    if container == "mp4":
        args += ["-movflags", "frag_keyframe+empty_moov+default_base_moof"]
    return subprocess.check_output(args + ["-f", container, "pipe:1"])


async def main():
    payloads = {IDS[0]: tone("libopus", "webm"), IDS[1]: tone("aac", "mp4"), IDS[2]: b"invalid"}
    target = dl.SMBTarget.parse("smb://127.0.0.1/music/Library")
    calls = []
    blocked = asyncio.Event()

    async def details(video_id, media_type):
        calls.append(video_id)
        return NS(item_id=video_id, audio_format=NS(content_type=NS(value="m4a" if video_id == IDS[1] else "opus"),
                                                  sample_rate=48000))

    async def source(streamdetails):
        if streamdetails.item_id == IDS[3]:
            blocked.set()
            await asyncio.sleep(600)
        data = payloads[streamdetails.item_id]
        for start in range(0, len(data), 1024):
            yield data[start:start + 1024]

    provider = NS(instance_id="verification", logger=logging.getLogger("verification"),
                  mass=NS(storage_path="/verification", player_queues=NS(all=lambda: ())),
                  _get_online_stream_details=details, get_audio_stream=source,
                  _get_youtube_audio_stream=source)

    def manager():
        return dl.DownloadManager(provider, target, "music", "music-test-only", 1024 ** 3)

    def metadata(video_id):
        return {"youtube_id": video_id, "title": "Tést Song", "artist": "Test Artist", "album": "Test Album",
                "duration": 3, "external_ids": [["isrc", "ZZTEST0000001"]]}

    current = manager()
    await current.start()
    try:
        for video_id in (*IDS[:2], IDS[0]):
            current.submit(video_id, metadata(video_id))
        async with asyncio.timeout(90):
            while True:
                def statuses():
                    with current.queue.connect() as db:
                        return db.execute("SELECT status FROM jobs").fetchall()
                states = await dl.blocking(statuses)
                if states == [("done",), ("done",)]:
                    break
                await asyncio.sleep(0.1)
    finally:
        await current.stop()
    assert sorted(calls) == sorted(IDS[:2]), calls

    restarted = manager()
    await dl.blocking(restarted.queue.initialize)
    assert not await dl.blocking(restarted.queue.enqueue, IDS[0], metadata(IDS[0]))
    assert await dl.blocking(restarted.queue.claim, float("inf")) is None
    await dl.blocking(restarted.store.prepare)
    saved_hashes = []
    for video_id, extension in zip(IDS, ("opus", "m4a")):
        path = ntpath.join(target.directory, video_id + "." + extension)

        def read():
            with restarted.store.open(path, "rb") as file:
                return file.read()

        data = await dl.blocking(read)
        saved_hashes.append(hashlib.sha256(data).hexdigest())
        probe = subprocess.run(["ffprobe", "-v", "error", "-i", "pipe:0", "-show_streams", "-show_format",
                                "-of", "json"], input=data, capture_output=True, check=True)
        info = json.loads(probe.stdout)
        tags = {k.lower(): v for s in [*info["streams"], info["format"]] for k, v in s.get("tags", {}).items()}
        assert tags["title"] == "Tést Song" and tags["artist"] == "Test Artist"
        assert info["streams"][0]["codec_name"] == ("opus" if extension == "opus" else "aac")

    # Indexed SMB playback must decode both codecs without a YouTube lookup or space check.
    from contextlib import aclosing
    from music_assistant.providers.ytmusic_free import smb_cache
    from music_assistant.providers.ytmusic_free import YoutubeMusicFreeProvider
    from music_assistant_models.enums import MediaType, ContentType
    provider._downloads = restarted
    provider.config = NS(get_value=lambda key: True)
    provider._apply_stream_trim = YoutubeMusicFreeProvider._apply_stream_trim
    initial_calls = list(calls)
    restarted.store.minimum_free = 2 ** 63
    for video_id in IDS[:2]:
        entry = await restarted.playback.lookup(video_id)
        assert entry and entry["metadata"]["sha256"]
        stream_details = await YoutubeMusicFreeProvider.get_stream_details(provider, video_id, MediaType.TRACK)
        assert stream_details.audio_format.content_type == ContentType.PCM_F32LE
        async with aclosing(YoutubeMusicFreeProvider.get_audio_stream(provider, stream_details)) as stream:
            pcm = b"".join([part async for part in stream])
        assert abs(len(pcm) / smb_cache.PCM_BYTES_PER_SECOND - 3) < .1
    assert calls == initial_calls
    restarted.store.minimum_free = 1024 ** 3
    # Native Linux subprocess cleanup must also work when a player stops mid-song.
    source_closed = asyncio.Event()
    async def interrupted_source():
        try:
            yield tone("libopus", "webm", duration=30)
            await asyncio.sleep(60)
        finally:
            source_closed.set()
    decoder = smb_cache.decode(interrupted_source())
    assert await asyncio.wait_for(anext(decoder), 5)
    await asyncio.wait_for(decoder.aclose(), 3)
    assert source_closed.is_set()
    files = await dl.blocking(restarted.store.client.listdir, target.directory, **restarted.store.options)
    assert not any(name.endswith(".json") for name in files)

    # Missing indexed file falls back online and can be downloaded again after listening.
    entry = await restarted.playback.lookup(IDS[0])
    path = ntpath.join(target.directory, entry["filename"])
    await dl.blocking(restarted.store.client.remove, path, **restarted.store.options)
    async with aclosing(restarted.playback.stream(entry)) as stream:
        pcm = b"".join([part async for part in stream])
    assert abs(len(pcm) / smb_cache.PCM_BYTES_PER_SECOND - 3) < .1
    assert await dl.blocking(restarted.queue.cached, IDS[0]) is None
    assert await dl.blocking(restarted.queue.enqueue, IDS[0], metadata(IDS[0]))
    await restarted.process(await dl.blocking(restarted.queue.claim, float("inf")))
    assert await dl.blocking(restarted.queue.cached, IDS[0])

    # A same-byte rewrite changes mtime: checksum revalidation must update the DB.
    entry = await restarted.playback.lookup(IDS[1])
    path = ntpath.join(target.directory, entry["filename"])
    def rewrite():
        with restarted.store.open(path, "rb") as file:
            data = file.read()
        with restarted.store.open(path, "wb") as file:
            file.write(data)
    await dl.blocking(rewrite)
    async with aclosing(restarted.playback.stream(entry)) as stream:
        pcm = b"".join([part async for part in stream])
    assert abs(len(pcm) / smb_cache.PCM_BYTES_PER_SECOND - 3) < .1
    updated = await dl.blocking(restarted.queue.cached, IDS[1])
    assert updated["mtime_ns"] != entry["mtime_ns"]

    # A failed decode leaves no published or partial audio, then a bounded retry succeeds.
    await dl.blocking(restarted.queue.enqueue, IDS[2], metadata(IDS[2]))
    await restarted.process(await dl.blocking(restarted.queue.claim, float("inf")))
    assert not await dl.blocking(restarted.store.exists, ntpath.join(target.directory, IDS[2] + ".opus"))
    payloads[IDS[2]] = payloads[IDS[0]]
    await restarted.process(await dl.blocking(restarted.queue.claim, float("inf")))
    assert await dl.blocking(restarted.store.exists, ntpath.join(target.directory, IDS[2] + ".opus"))

    original_reserve = restarted.store.minimum_free
    restarted.store.minimum_free = 2 ** 63
    try:
        await dl.blocking(restarted.store.check_space)
        raise AssertionError("low-space guard did not run")
    except dl.DownloadError:
        pass
    restarted.store.minimum_free = original_reserve
    await restarted.stop()

    cancelled = manager()
    await cancelled.start()
    cancelled.submit(IDS[3], metadata(IDS[3]))
    await asyncio.wait_for(blocked.wait(), 30)
    await cancelled.stop()
    check = manager()
    try:
        with check.queue.connect() as db:
            assert db.execute("SELECT status,attempts FROM jobs WHERE video_id=?", (IDS[3],)).fetchone() == ("pending", 0)
        remaining = await dl.blocking(check.store.client.listdir, target.staging, **check.store.options)
        assert remaining == [], remaining
        assert all(path.suffix == ".sqlite3" for path in Path("/verification").rglob("*") if path.is_file())
    finally:
        await check.stop()
    print(json.dumps({"smb_integration": "passed", "codecs": ["opus", "aac"], "deduplication": True,
                      "restart": True, "failed_validation_retry": True, "cancellation": True,
                      "remote_space_guard": True, "remote_staging_empty": True, "no_local_audio": True,
                      "indexed_cache_playback": True, "missing_file_online_fallback": True,
                      "missing_file_redownload": True, "changed_file_revalidation": True, "no_json_files": True,
                      "partial_decoder_cleanup": True,
                      "saved_audio_sha256": saved_hashes}))


asyncio.run(main())
