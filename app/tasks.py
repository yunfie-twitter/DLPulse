import re
import os
import shutil
import zipfile
import subprocess
from pathlib import Path
import requests
from celery import Celery, group
import yt_dlp
import redis
import logging
from app.config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

celery_app = Celery(
    "tasks",
    broker=f"redis://{settings.redis_host}:{settings.redis_port}/0",
    backend=f"redis://{settings.redis_host}:{settings.redis_port}/0",
)

redis_sync = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    decode_responses=True,
)

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def convert_webm_to_mp3(input_file: Path, output_file: Path):
    try:
        cmd = [
            "ffmpeg", "-y",
            "-i", str(input_file),
            "-vn",
            "-acodec", "libmp3lame",
            "-ab", "192k",
            str(output_file),
        ]
        result = subprocess.run(cmd, capture_output=True)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.decode())
    except Exception as e:
        logger.error(f"[convert_webm_to_mp3] 変換失敗: {input_file} -> {e}")
        raise

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def download_single_video(self, url, video_id, title, download_dir, audio_only=False):
    try:
        target_dir = Path(download_dir) / video_id
        target_dir.mkdir(exist_ok=True, parents=True)
        temp_output = str(target_dir / f"{video_id}.%(ext)s")

        ydl_opts = {
            "format": "bestaudio[ext=webm]/bestaudio/best" if audio_only else "bestvideo+bestaudio/best",
            "outtmpl": temp_output,
            "quiet": True,
        }

        def progress_hook(d):
            if d["status"] == "downloading":
                total_bytes = d.get("total_bytes") or d.get("total_bytes_estimate")
                downloaded_bytes = d.get("downloaded_bytes", 0)
                speed = d.get("speed", 0.0)
                eta = d.get("eta", 0)
                percent = round(downloaded_bytes / total_bytes * 100, 1) if total_bytes else 0
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": f"downloading {title}",
                        "progress": percent,
                        "speed": f"{speed / (1024 * 1024):.1f}MB/s" if speed else "",
                        "eta": eta,
                    },
                )

        ydl_opts["progress_hooks"] = [progress_hook]

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        if audio_only:
            temp_webm = next((f for f in target_dir.iterdir() if f.suffix == ".webm"), None)
            if not temp_webm:
                raise FileNotFoundError(f"音声ファイルが見つかりません: {title}")
            final_mp3_path = target_dir / f"{sanitize_filename(title)}.mp3"
            if final_mp3_path.exists():
                final_mp3_path.unlink()
            convert_webm_to_mp3(temp_webm, final_mp3_path)
            temp_webm.unlink(missing_ok=True)
            return str(final_mp3_path)

        else:
            temp_file = next(target_dir.iterdir(), None)
            if not temp_file:
                raise FileNotFoundError(f"動画ファイルが見つかりません: {title}")
            return str(temp_file)

    except Exception as e:
        logger.error(f"[download_single_video] ダウンロード失敗: {url} ({e})")
        return None

@celery_app.task(bind=True)
def download_video_task(self, url: str, audio_only: bool = False, webhook_url: str = None):
    base_download_dir = Path("./downloads").resolve()
    base_download_dir.mkdir(exist_ok=True)

    try:
        with yt_dlp.YoutubeDL({"extract_flat": True, "quiet": True}) as ydl:
            info_dict = ydl.extract_info(url, download=False)
    except Exception as e:
        self.update_state(state="FAILURE", meta={"error": str(e)})
        logger.error(f"[download_video_task] 情報取得失敗: {url} ({e})")
        raise

    if info_dict.get("_type") == "playlist":
        playlist_title = sanitize_filename(info_dict.get("title", "playlist"))
        playlist_dir = base_download_dir / playlist_title
        playlist_dir.mkdir(exist_ok=True)

        entries = [
            entry for entry in info_dict.get("entries", [])
            if entry.get("url") or entry.get("webpage_url")
        ]
        if not entries:
            raise ValueError("プレイリストに有効な動画がありません。")

        job_group = group(
            download_single_video.s(
                entry.get("url") or entry.get("webpage_url"),
                entry.get("id") or str(hash(entry.get("url") or entry.get("webpage_url"))),
                entry.get("title", "unknown"),
                str(playlist_dir),
                audio_only
            )
            for entry in entries
        )

        results = job_group.apply_async().get()
        downloaded_files = [Path(f) for f in results if f]

        if not downloaded_files:
            raise RuntimeError("プレイリスト内の動画が全て失敗しました。")

        zip_filename = f"{playlist_title}.zip"
        zip_filepath = base_download_dir / zip_filename

        with zipfile.ZipFile(zip_filepath, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            for fpath in downloaded_files:
                relpath = fpath.relative_to(base_download_dir)
                zipf.write(fpath, arcname=relpath)

        shutil.rmtree(playlist_dir, ignore_errors=True)
        redis_sync.set(self.request.id, zip_filename)

        if webhook_url:
            try:
                requests.post(
                    webhook_url,
                    json={"task_id": self.request.id, "status": "completed", "file_name": zip_filename},
                    timeout=5
                )
            except Exception as e:
                logger.warning(f"[webhook] プレイリスト送信失敗: {e}")

        return zip_filename

    else:
        video_id = info_dict.get("id", self.request.id)
        orig_title = info_dict.get("title", "unknown")
        safe_title = sanitize_filename(orig_title)
        target_dir = base_download_dir / video_id
        target_dir.mkdir(exist_ok=True)

        temp_output = str(target_dir / f"{self.request.id}.%(ext)s")

        ydl_opts = {
            "format": "bestaudio[ext=webm]/bestaudio/best" if audio_only else "bestvideo+bestaudio/best",
            "outtmpl": temp_output,
            "quiet": True,
        }

        def progress_hook(d):
            if d["status"] == "downloading":
                total_bytes = d.get("total_bytes") or d.get("total_bytes_estimate")
                downloaded_bytes = d.get("downloaded_bytes", 0)
                speed = d.get("speed", 0.0)
                eta = d.get("eta", 0)
                percent = round(downloaded_bytes / total_bytes * 100, 1) if total_bytes else 0
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "status": "downloading",
                        "progress": percent,
                        "speed": f"{speed / (1024 * 1024):.1f}MB/s" if speed else "",
                        "eta": eta,
                    },
                )

        ydl_opts["progress_hooks"] = [progress_hook]

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        if audio_only:
            temp_webm = next((f for f in target_dir.iterdir() if f.suffix == ".webm"), None)
            if not temp_webm or not temp_webm.exists():
                raise FileNotFoundError(f"Webm file not found for {url}")

            final_mp3_path = target_dir / f"{safe_title}.mp3"
            convert_webm_to_mp3(temp_webm, final_mp3_path)
            temp_webm.unlink(missing_ok=True)
            redis_sync.set(self.request.id, f"{video_id}/{final_mp3_path.name}")
            final_name = final_mp3_path.name
        else:
            temp_file = next(target_dir.iterdir(), None)
            if not temp_file:
                raise FileNotFoundError("Downloaded file not found")
            final_filepath = target_dir / f"{safe_title}{temp_file.suffix}"
            temp_file.rename(final_filepath)
            redis_sync.set(self.request.id, f"{video_id}/{final_filepath.name}")
            final_name = final_filepath.name

        if webhook_url:
            try:
                requests.post(
                    webhook_url,
                    json={"task_id": self.request.id, "status": "completed", "file_name": f"{video_id}/{final_name}"},
                    timeout=5
                )
            except Exception as e:
                logger.warning(f"[webhook] 単一動画送信失敗: {e}")

        return f"{video_id}/{final_name}"
