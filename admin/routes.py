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
        # Kasutajad
        users = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') AS today,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') AS week
            FROM users WHERE deleted_at IS NULL
        """)

        # Ketide coverage
        chains = await conn.fetch("""
            SELECT
                p.chain,
                COUNT(DISTINCT p.id) AS total,
                COUNT(DISTINCT p.id) FILTER (WHERE pr.id IS NOT NULL) AS with_price,
                COUNT(DISTINCT p.id) FILTER (WHERE p.image_url IS NOT NULL AND p.image_url != '') AS with_image
            FROM products p
            LEFT JOIN prices pr ON pr.product_id = p.id
            GROUP BY p.chain
            ORDER BY total DESC
        """)

        # Grupeerimata top 10
        ungrouped = await conn.fetch("""
            SELECT sub_code, COUNT(*) AS cnt
            FROM products
            WHERE id NOT IN (SELECT product_id FROM product_group_members)
            AND sub_code IS NOT NULL
            GROUP BY sub_code
            ORDER BY cnt DESC
            LIMIT 10
        """)
        ungrouped_total = await conn.fetchval("""
            SELECT COUNT(*) FROM products
            WHERE id NOT IN (SELECT product_id FROM product_group_members)
        """)

        # Tooted ilma hinnata top 5
        no_price = await conn.fetch("""
            SELECT p.sub_code, COUNT(DISTINCT p.id) AS cnt
            FROM products p
            WHERE NOT EXISTS (SELECT 1 FROM prices pr WHERE pr.product_id = p.id)
            AND p.sub_code IS NOT NULL
            GROUP BY p.sub_code
            ORDER BY cnt DESC
            LIMIT 5
        """)

        # Integrity check
        integrity_count = await conn.fetchval("""
            SELECT COUNT(DISTINCT p.id)
            FROM products p
            JOIN prices pr ON pr.product_id = p.id
            JOIN stores s ON s.id = pr.store_id
            WHERE p.chain = 'coop' AND s.name ILIKE '%Rimi%'
            AND p.id NOT IN (
                SELECT DISTINCT p2.id FROM products p2
                JOIN prices pr2 ON pr2.product_id = p2.id
                JOIN stores s2 ON s2.id = pr2.store_id
                WHERE p2.chain = 'coop' AND s2.name NOT ILIKE '%Rimi%'
            )
        """)

        # Scraperите viimane aktiivsus
        scraper_rows = await conn.fetch("""
            SELECT
                chain,
                MAX(last_seen_utc) AS last_update,
                COUNT(*) FILTER (WHERE last_seen_utc >= NOW() - INTERVAL '24 hours') AS updated_today
            FROM products
            GROUP BY chain
            ORDER BY last_update DESC NULLS LAST
        """)

        # NULL sub_code
        null_subcode = await conn.fetchval("SELECT COUNT(*) FROM products WHERE sub_code IS NULL")

    # HTML ehitamine
    def pct(a, b):
        return f"{round(a/b*100,1)}%" if b else "0%"

    def status_color(val, warn=80, ok=95):
        if val >= ok: return "#2ecc71"
        if val >= warn: return "#f39c12"
        return "#e74c3c"

    chains_html = ""
    for r in chains:
        pp = round(r['with_price']/r['total']*100, 1) if r['total'] else 0
        ip = round(r['with_image']/r['total']*100, 1) if r['total'] else 0
        chains_html += f"""
        <tr>
            <td><b>{r['chain']}</b></td>
            <td>{r['total']:,}</td>
            <td style="color:{status_color(pp)}">{r['with_price']:,} ({pp}%)</td>
            <td style="color:{status_color(ip)}">{r['with_image']:,} ({ip}%)</td>
        </tr>"""

    ungrouped_html = "".join(
        f"<tr><td>{r['sub_code']}</td><td style='color:#e74c3c'>{r['cnt']}</td></tr>"
        for r in ungrouped
    )

    no_price_html = "".join(
        f"<tr><td>{r['sub_code']}</td><td style='color:#e74c3c'>{r['cnt']}</td></tr>"
        for r in no_price
    )

    scraper_html = ""
    for r in scraper_rows:
        lu = r['last_update']
        if lu:
            import datetime
            diff = datetime.datetime.now(datetime.timezone.utc) - lu
            hours = diff.total_seconds() / 3600
            color = "#2ecc71" if hours < 25 else ("#f39c12" if hours < 48 else "#e74c3c")
            lu_str = lu.strftime("%d.%m %H:%M")
        else:
            color = "#e74c3c"
            lu_str = "puudub"
        scraper_html += f"""
        <tr>
            <td><b>{r['chain']}</b></td>
            <td style="color:{color}">{lu_str}</td>
            <td>{r['updated_today']:,}</td>
        </tr>"""

    integrity_color = "#2ecc71" if integrity_count == 0 else "#e74c3c"
    integrity_text = "OK ✅" if integrity_count == 0 else f"⚠️ {integrity_count} toodet"

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Seivy Admin</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #f5f5f5; margin: 0; padding: 16px; color: #222; }}
  h1 {{ color: #C94B7C; margin-bottom: 4px; }}
  h2 {{ font-size: 1rem; color: #555; margin: 24px 0 8px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 8px; }}
  .card {{ background: white; border-radius: 10px; padding: 16px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  .card .num {{ font-size: 2rem; font-weight: 700; color: #C94B7C; }}
  .card .label {{ font-size: 0.8rem; color: #888; margin-top: 2px; }}
  table {{ width: 100%; background: white; border-radius: 10px;
           box-shadow: 0 1px 4px rgba(0,0,0,0.08); border-collapse: collapse; margin-bottom: 16px; }}
  th {{ background: #f0f0f0; padding: 8px 12px; text-align: left; font-size: 0.8rem; color: #555; }}
  td {{ padding: 8px 12px; border-top: 1px solid #f0f0f0; font-size: 0.9rem; }}
  .integrity {{ background: white; border-radius: 10px; padding: 16px;
                box-shadow: 0 1px 4px rgba(0,0,0,0.08); margin-bottom: 16px; }}
  .section-link {{ font-size:0.8rem; color:#C94B7C; text-decoration:none; float:right; margin-top:4px; }}
</style>
</head><body>
<h1>Seivy Admin</h1>

<h2>Kasutajad</h2>
<div class="grid">
  <div class="card"><div class="num">{users['total']}</div><div class="label">Kokku</div></div>
  <div class="card"><div class="num">{users['today']}</div><div class="label">Täna uued</div></div>
  <div class="card"><div class="num">{users['week']}</div><div class="label">Viimane 7 päeva</div></div>
</div>

<h2>Integrity check</h2>
<div class="integrity">
  Coop tooted ainult Rimi poodides: <b style="color:{integrity_color}">{integrity_text}</b>
</div>

<h2>Scraperите aktiivsus</h2>
<table>
  <tr><th>Kett</th><th>Viimane update</th><th>Täna uuendatud</th></tr>
  {scraper_html}
</table>

<h2>Ketide coverage</h2>
<table>
  <tr><th>Kett</th><th>Tooted</th><th>Hinnaga</th><th>Pildiga</th></tr>
  {chains_html}
</table>

<h2>Grupeerimata tooted — top 10 <small style="color:#e74c3c">({ungrouped_total} kokku)</small></h2>
<table>
  <tr><th>sub_code</th><th>Arv</th></tr>
  {ungrouped_html}
</table>

<h2>Tooted ilma hinnata — top 5</h2>
<table>
  <tr><th>sub_code</th><th>Arv</th></tr>
  {no_price_html}
</table>

<h2>NULL sub_code</h2>
<div class="integrity">
  Tooteid ilma sub_code'ita: <b style="color:{'#2ecc71' if null_subcode == 0 else '#e74c3c'}">{null_subcode}</b>
</div>

<p style="color:#aaa;font-size:0.75rem;text-align:center">Seivy Admin · {__import__('datetime').datetime.now().strftime('%d.%m.%Y %H:%M')}</p>
</body></html>"""
    return HTMLResponse(html)


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
