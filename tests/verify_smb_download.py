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


def tone(codec, container):
    args = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
            "sine=frequency=440:duration=3", "-c:a", codec]
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
                  get_stream_details=details, get_audio_stream=source)

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
    assert calls == list(IDS[:2]), calls

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
                      "saved_audio_sha256": saved_hashes}))


asyncio.run(main())
