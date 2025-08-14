# api/upload_image.py
from io import BytesIO
import httpx
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Header
from fastapi.responses import JSONResponse
import mimetypes

from settings import ADMIN_UPLOAD_TOKEN
from services.r2_client import (
    generate_r2_key,
    image_exists_in_r2,
    upload_image_to_r2,
    r2_public_url,  # if you export it from settings as in your helper
)

router = APIRouter(prefix="/api", tags=["uploads"])

# 10 MB default cap (adjust as needed)
MAX_BYTES = 10 * 1024 * 1024
ALLOWED_MIME_PREFIXES = ("image/",)

def _ensure_authorized(authorization: str | None):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1]
    if token != ADMIN_UPLOAD_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")

def _validate_image(content: bytes, filename: str | None, mime_hint: str | None) -> tuple[str, str]:
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > MAX_BYTES:
        raise HTTPException(status_code=413, detail=f"File too large (>{MAX_BYTES} bytes)")

    # prefer client-provided content-type, fall back to guess from filename
    content_type = (mime_hint or "").lower() or mimetypes.guess_type(filename or "")[0] or "application/octet-stream"
    if not any(content_type.startswith(pfx) for pfx in ALLOWED_MIME_PREFIXES):
        raise HTTPException(status_code=415, detail=f"Unsupported content-type: {content_type}")

    key = generate_r2_key(content, original_filename=filename or "")
    return key, content_type

@router.post("/upload-image")
async def upload_image(
    authorization: str | None = Header(default=None),
    file: UploadFile | None = File(default=None, description="Image file (multipart/form-data)"),
    remote_url: str | None = Form(default=None, description="Alternatively, a URL to fetch"),
):
    """
    Manual image upload:
      - send multipart `file`, or
      - send `remote_url` (server fetches the image)

    Returns: { "key": "...", "url": "https://...", "deduplicated": true/false }
    """
    _ensure_authorized(authorization)

    if not file and not remote_url:
        raise HTTPException(status_code=400, detail="Provide either file or remote_url")

    # Read bytes
    if file:
        content = await file.read()
        filename = file.filename
        mime_hint = file.content_type
    else:
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
                resp = await client.get(remote_url)
                resp.raise_for_status()
                content = resp.content
                mime_hint = resp.headers.get("content-type", "").split(";")[0]
                # try to infer filename from URL
                filename = remote_url.split("?")[0].split("/")[-1] or "image"
        except httpx.HTTPError as e:
            raise HTTPException(status_code=400, detail=f"Fetching remote_url failed: {e}") from e

    key, content_type = _validate_image(content, filename, mime_hint)

    # Deduplicate
    if image_exists_in_r2(key):
        return JSONResponse({"key": key, "url": r2_public_url(key), "deduplicated": True})

    # Upload
    url = upload_image_to_r2(content, key, content_type)
    return JSONResponse({"key": key, "url": url, "deduplicated": False})
