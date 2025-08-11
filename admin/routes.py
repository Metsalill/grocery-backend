# admin/routes.py
import os, shutil
from fastapi import APIRouter, Depends, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, JSONResponse
from settings import IMAGES_DIR, MAX_UPLOAD_MB, CDN_BASE_URL
from .security import basic_guard

router = APIRouter()

@router.get("/", response_class=HTMLResponse, dependencies=[Depends(basic_guard)])
async def dashboard(request: Request):
    if getattr(request.app.state, "db", None) is None:
        return HTMLResponse("<h2>DB not ready yet. Try again in a few seconds.</h2>", status_code=503)
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT
                   product,
                   COALESCE(manufacturer,'') AS manufacturer,
                   COALESCE(amount,'')       AS amount
            FROM prices
            WHERE (image_url IS NULL OR image_url = '' OR image_url = 'missing.jpg')
               OR note = 'Kontrolli visuaali!'
            ORDER BY product
        """)
    html = "<h2>Missing Product Images</h2><ul>"
    for r in rows:
        p, m, a = r["product"], r["manufacturer"], r["amount"]
        html += f"""
        <li><b>{p}</b>{' · '+m if m else ''}{' · '+a if a else ''}
            <form action="/upload" method="post" enctype="multipart/form-data" style="margin-top:6px">
                <input type="hidden" name="product" value="{p}"/>
                <input type="hidden" name="manufacturer" value="{m}"/>
                <input type="hidden" name="amount" value="{a}"/>
                <input type="file" name="image" accept="image/*" required/>
                <button type="submit">Upload</button>
            </form>
        </li>"""
    html += "</ul>"
    return html

@router.post("/upload", dependencies=[Depends(basic_guard)])
async def upload_image(
    request: Request,
    product: str = Form(...),
    image: UploadFile = Form(...),
    manufacturer: str = Form(""),
    amount: str = Form("")
):
    def wants_html(req: Request) -> bool:
        accept = (req.headers.get("accept") or "").lower()
        return "text/html" in accept and "application/json" not in accept

    try:
        cl = request.headers.get("content-length")
        if cl and int(cl) > MAX_UPLOAD_MB * 1024 * 1024:
            raise HTTPException(413, f"Image too large (>{MAX_UPLOAD_MB}MB)")

        safe_base = (product.replace("/", "_").replace("\\", "_").replace(" ", "_").strip())
        ext = os.path.splitext(image.filename or "")[1].lower() or ".jpg"
        filename = f"{safe_base}{ext}"

        file_path = os.path.join(IMAGES_DIR, filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(image.file, buffer)

        try:
            if os.path.getsize(file_path) > MAX_UPLOAD_MB * 1024 * 1024:
                os.remove(file_path)
                raise HTTPException(413, f"Image too large (>{MAX_UPLOAD_MB}MB)")
        except Exception:
            pass

        image_path = f"/static/images/{filename}"
        image_url = f"{CDN_BASE_URL.rstrip('/')}{image_path}" if CDN_BASE_URL else image_path

        if getattr(request.app.state, "db", None) is None:
            raise HTTPException(status_code=503, detail="Database not ready")

        async with request.app.state.db.acquire() as conn:
            if manufacturer or amount:
                status_txt = await conn.execute("""
                    UPDATE prices
                       SET image_url = $4,
                           note = CASE WHEN note = 'Kontrolli visuaali!' THEN '' ELSE note END
                     WHERE LOWER(product) = LOWER($1)
                       AND LOWER(COALESCE(manufacturer,'')) = LOWER($2)
                       AND LOWER(COALESCE(amount,'')) = LOWER($3)
                """, product.strip(), manufacturer.strip(), amount.strip(), image_url)
            else:
                status_txt = await conn.execute("""
                    UPDATE prices
                       SET image_url = $2,
                           note = CASE WHEN note = 'Kontrolli visuaali!' THEN '' ELSE note END
                     WHERE LOWER(product) = LOWER($1)
                """, product.strip(), image_url)

        updated_rows = 0
        try:
            updated_rows = int((status_txt or "0").split()[-1])
        except Exception:
            pass

        if wants_html(request):
            return HTMLResponse(f"""
                <h2>✅ Image uploaded</h2>
                <p><b>Product:</b> {product}</p>
                <p><b>Rows updated:</b> {updated_rows}</p>
                <p><img src="{image_url}" alt="{product}" style="max-width:520px;height:auto;border:1px solid #eee"/></p>
                <p><a href="/">← Back to Missing Product Images</a></p>
            """)

        return JSONResponse({"status": "success", "product": product, "image_url": image_url,
                             "rows_updated": updated_rows}, status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        if wants_html(request):
            return HTMLResponse(f"<h2>❌ Upload failed</h2><pre>{str(e)}</pre><p><a href='/'>← Back</a></p>", status_code=500)
        raise
