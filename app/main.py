import os
import sys
import platform
import time
import json
import hashlib
import asyncio
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import logging
import requests

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from celery.result import AsyncResult

import yt_dlp
import redis.asyncio as redis_client
from app.config import settings
from app.tasks import download_video_task, celery_app

app = FastAPI(title="Video Download API", version="1.0.1")

CACHE_TTL_SECONDS = 300
DOWNLOAD_DIR = Path("./downloads").resolve()
DOWNLOAD_DIR.mkdir(exist_ok=True, parents=True)
executor = ThreadPoolExecutor(max_workers=4)
SHA256_CACHE_PREFIX = "sha256:"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("yt-dlp-api")

redis = redis_client.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    decode_responses=True,
)

class VideoInfoRequest(BaseModel):
    url: str

class DownloadRequest(BaseModel):
    url: str
    quality: str = "bestvideo+bestaudio/best"
    webhook_url: str | None = None
    audio_only: bool = False

def url_to_cache_key(url: str) -> str:
    return f"video_info_cache:{hashlib.sha256(url.encode()).hexdigest()}"

async def safe_redis_get(key: str):
    try:
        return await redis.get(key)
    except Exception as e:
        logger.warning(f"Redis GETエラー: {e}")
        return None

async def safe_redis_set(key: str, value, ex=None):
    try:
        await redis.set(key, value, ex=ex)
    except Exception as e:
        logger.warning(f"Redis SETエラー: {e}")

def safe_send_webhook(webhook_url: str, payload: dict):
    if not webhook_url:
        logger.info("Webhook URL が指定されていません。送信をスキップ。")
        return
    if not webhook_url.startswith(("http://", "https://")):
        logger.warning(f"無効なWebhook URL: {webhook_url}")
        return

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info(f"Webhook送信成功: {webhook_url}")
    except Exception as e:
        logger.warning(f"Webhook送信に失敗: {e}")

def calculate_sha256(file_path: Path) -> str:
    hash_sha256 = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

async def calculate_sha256_async(file_path: Path) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, calculate_sha256, file_path)

async def get_cached_sha256(file_path: Path, task_id: str):
    cache_key = f"{SHA256_CACHE_PREFIX}{task_id}"
    sha = await safe_redis_get(cache_key)
    if sha and sha != "CALCULATING":
        return sha
    if not sha:
        await safe_redis_set(cache_key, "CALCULATING", ex=3600)
    sha_val = await calculate_sha256_async(file_path)
    await safe_redis_set(cache_key, sha_val, ex=3600)
    return sha_val

async def extract_info_async(url: str, ydl_opts: dict):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, lambda: yt_dlp.YoutubeDL(ydl_opts).extract_info(url, download=False))

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    response.headers["X-Process-Time"] = f"{time.time() - start_time:.3f}s"
    return response

@app.get("/")
async def root():
    try:
        ytdlp_version = yt_dlp.version.__version__
    except Exception:
        ytdlp_version = "unknown"

    try:
        keys = await redis.keys("celery-task-meta-*")
        pending_count = sum(
            1 for key in keys if AsyncResult(key[len("celery-task-meta-"):], app=celery_app).state == "PENDING"
        )
    except Exception as e:
        logger.warning(f"タスク数取得失敗: {e}")
        pending_count = -1

    return {
        "app_name": "Video Download API",
        "version": "1.0.1",
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "yt_dlp": ytdlp_version,
        "queued_tasks_pending": pending_count,
    }

@app.post("/video-info/")
async def video_info(request: VideoInfoRequest):
    cache_key = url_to_cache_key(request.url)
    cached = await safe_redis_get(cache_key)
    if cached:
        return json.loads(cached)

    ydl_opts = {"skip_download": True, "quiet": True, "extract_flat": True}
    try:
        info = await extract_info_async(request.url, ydl_opts)
    except Exception as e:
        logger.error(f"動画情報取得失敗: {e}")
        raise HTTPException(status_code=500, detail=f"yt-dlp error: {e}")

    if info.get("_type") == "playlist":
        videos = [
            {
                "id": entry.get("id"),
                "title": entry.get("title"),
                "url": entry.get("url"),
                "duration": entry.get("duration"),
                "thumbnail": entry.get("thumbnail"),
            }
            for entry in info.get("entries", [])
        ]
        result = {"title": info.get("title"), "id": info.get("id"), "entries": videos}
    else:
        formats = info.get("formats", [])
        format_list = [
            {
                "format_id": f.get("format_id"),
                "ext": f.get("ext"),
                "format_note": f.get("format_note"),
                "filesize": f.get("filesize"),
                "fps": f.get("fps"),
                "acodec": f.get("acodec"),
                "vcodec": f.get("vcodec"),
                "height": f.get("height"),
                "width": f.get("width"),
            }
            for f in formats
        ]
        result = {
            "title": info.get("title"),
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "upload_date": info.get("upload_date"),
            "uploader": info.get("uploader"),
            "formats": format_list,
        }

    await safe_redis_set(cache_key, json.dumps(result), ex=CACHE_TTL_SECONDS)
    return result

@app.post("/download/")
async def download(request: DownloadRequest):
    try:
        task = download_video_task.apply_async(
            args=[request.url, request.audio_only],
            kwargs={"webhook_url": request.webhook_url}
        )
        logger.info(f"ダウンロードタスク開始: {request.url} → task_id={task.id}")
        return {"task_id": task.id, "status": "started"}
    except Exception as e:
        logger.error(f"タスク作成失敗: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    task = AsyncResult(task_id, app=celery_app)
    if task.state == "PENDING":
        return {"status": "pending", "progress": 0}
    elif task.state == "PROGRESS":
        meta = task.info or {}
        return {
            "status": meta.get("status", "progress"),
            "progress": meta.get("progress", 0),
            "speed": meta.get("speed", ""),
            "eta": meta.get("eta", 0),
        }
    elif task.state == "SUCCESS":
        filename = await safe_redis_get(task_id)
        if not filename:
            filename = "unknown"
        file_path = DOWNLOAD_DIR / Path(filename)
        sha_val = await safe_redis_get(f"{SHA256_CACHE_PREFIX}{task_id}")
        if sha_val is None or sha_val == "CALCULATING":
            asyncio.create_task(get_cached_sha256(file_path, task_id))
        resp = {
            "status": "success",
            "progress": 100,
            "file_name": file_path.name,
            "file_url": f"/download-file/?id={task_id}",
        }
        if sha_val and sha_val != "CALCULATING":
            resp["sha256"] = sha_val
        return resp
    elif task.state == "FAILURE":
        return {"status": "failure", "error": str(task.info)}
    else:
        return {"status": task.state}

@app.get("/download-file/")
async def download_file(id: str = Query(..., description="ジョブID (task_id)")):
    filename = await safe_redis_get(id)
    if not filename:
        raise HTTPException(status_code=404, detail="File not found for this job ID")
    file_path = (DOWNLOAD_DIR / Path(filename)).resolve()
    if not str(file_path).startswith(str(DOWNLOAD_DIR)):
        raise HTTPException(status_code=400, detail="Invalid file path")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    ext = file_path.suffix.lower()
    media_type = {
        ".mp4": "video/mp4",
        ".webm": "video/webm",
        ".mp3": "audio/mpeg",
        ".zip": "application/zip",
    }.get(ext, "application/octet-stream")

    return FileResponse(path=str(file_path), filename=file_path.name, media_type=media_type)

async def auto_delete_expired_files():
    keys = await redis.keys("auto_delete:*")
    now = int(time.time())
    for key in keys:
        ts = await safe_redis_get(key)
        if ts and int(ts) <= now:
            task_id = key[len("auto_delete:"):]
            filename = await safe_redis_get(task_id)
            if filename:
                file_path = DOWNLOAD_DIR / filename
                if file_path.exists():
                    try:
                        file_path.unlink()
                        await redis.delete(task_id)
                        await redis.delete(key)
                        logger.info(f"削除完了: {file_path.name}")
                    except Exception as e:
                        logger.warning(f"削除失敗: {e}")

async def schedule_auto_delete():
    while True:
        try:
            await auto_delete_expired_files()
        except Exception as e:
            logger.warning(f"Auto-delete error: {e}")
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_auto_delete())
    logger.info("自動削除タスクを開始しました。")
