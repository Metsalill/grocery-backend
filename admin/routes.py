# admin/routes.py
import os, shutil, datetime
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
  .analytics-link {{ display: inline-block; margin-top: 8px; padding: 8px 16px;
                     background: #FF9100; color: white; border-radius: 8px;
                     text-decoration: none; font-weight: 600; font-size: 0.9rem; }}
</style>
</head><body>
<h1>Seivy Admin</h1>
<a class="analytics-link" href="/admin/analytics">📊 Vaata Analytics Dashboardi</a>

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

<p style="color:#aaa;font-size:0.75rem;text-align:center">Seivy Admin · {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}</p>
</body></html>"""
    return HTMLResponse(html)


@router.get("/admin/analytics", response_class=HTMLResponse, dependencies=[Depends(basic_guard)])
async def analytics_dashboard(request: Request, chain: str = None, days: int = 30):
    if getattr(request.app.state, "db", None) is None:
        return HTMLResponse("<h2>DB not ready yet.</h2>", status_code=503)

    async with request.app.state.db.acquire() as conn:
        # Korvi võitjad ketiti
        basket_wins = await conn.fetch("""
            SELECT chain, COUNT(*) AS wins
            FROM analytics_events
            WHERE event_type = 'basket_win'
              AND created_at >= NOW() - ($1 || ' days')::INTERVAL
              AND ($2::text IS NULL OR chain = $2)
            GROUP BY chain
            ORDER BY wins DESC
        """, str(days), chain)

        # Korvi lisamised ketiti
        basket_adds = await conn.fetch("""
            SELECT
                a.chain,
                COUNT(*) AS adds
            FROM analytics_events a
            WHERE a.event_type = 'basket_add'
              AND a.created_at >= NOW() - ($1 || ' days')::INTERVAL
              AND ($2::text IS NULL OR a.chain = $2)
            GROUP BY a.chain
            ORDER BY adds DESC
        """, str(days), chain)

        # Populaarseimad tooted korvi lisamisel
        top_products = await conn.fetch("""
            SELECT
                a.product_id,
                p.name,
                COUNT(*) AS adds
            FROM analytics_events a
            LEFT JOIN products p ON p.id = a.product_id
            WHERE a.event_type = 'basket_add'
              AND a.product_id IS NOT NULL
              AND a.created_at >= NOW() - ($1 || ' days')::INTERVAL
              AND ($2::text IS NULL OR a.chain = $2)
            GROUP BY a.product_id, p.name
            ORDER BY adds DESC
            LIMIT 10
        """, str(days), chain)

        # Päevane aktiivsus viimased 14 päeva
        daily = await conn.fetch("""
            SELECT
                DATE(created_at) AS day,
                event_type,
                COUNT(*) AS cnt
            FROM analytics_events
            WHERE created_at >= NOW() - INTERVAL '14 days'
              AND ($1::text IS NULL OR chain = $1)
            GROUP BY DATE(created_at), event_type
            ORDER BY day DESC
        """, chain)

        # Kokku statistika
        totals = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'basket_add') AS total_adds,
                COUNT(*) FILTER (WHERE event_type = 'basket_win') AS total_wins,
                COUNT(*) FILTER (WHERE event_type = 'product_view') AS total_views,
                COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL) AS unique_users
            FROM analytics_events
            WHERE created_at >= NOW() - ($1 || ' days')::INTERVAL
              AND ($2::text IS NULL OR chain = $2)
        """, str(days), chain)

    # Chain filter nupud
    all_chains = ['selver', 'rimi', 'prisma', 'coop', 'maxima']
    chain_btns = '<a href="/admin/analytics" style="margin:4px;padding:6px 14px;background:{}; color:white;border-radius:20px;text-decoration:none;font-size:0.85rem">Kõik</a>'.format(
        '#FF9100' if not chain else '#ccc'
    )
    for c in all_chains:
        active = chain and chain.lower() == c.lower()
        chain_btns += f'<a href="/admin/analytics?chain={c}&days={days}" style="margin:4px;padding:6px 14px;background:{"#FF9100" if active else "#ccc"};color:white;border-radius:20px;text-decoration:none;font-size:0.85rem">{c.capitalize()}</a>'

    # Days filter nupud
    days_btns = ''
    for d in [7, 14, 30, 90]:
        active = days == d
        chain_param = f'&chain={chain}' if chain else ''
        days_btns += f'<a href="/admin/analytics?days={d}{chain_param}" style="margin:4px;padding:6px 14px;background:{"#FF9100" if active else "#ccc"};color:white;border-radius:20px;text-decoration:none;font-size:0.85rem">{d}p</a>'

    # Basket wins tabel
    wins_html = ""
    total_wins = sum(r['wins'] for r in basket_wins) or 1
    for r in basket_wins:
        pct = round(r['wins'] / total_wins * 100, 1)
        bar = f'<div style="height:8px;background:#FF9100;border-radius:4px;width:{pct}%"></div>'
        wins_html += f"<tr><td><b>{r['chain'] or '—'}</b></td><td>{r['wins']}</td><td style='width:40%'>{bar} {pct}%</td></tr>"

    if not wins_html:
        wins_html = "<tr><td colspan='3' style='color:#aaa'>Andmed puuduvad</td></tr>"

    # Top tooted tabel
    products_html = ""
    for i, r in enumerate(top_products):
        products_html += f"<tr><td style='color:#aaa'>{i+1}</td><td>{r['name'] or f'ID {r[\"product_id\"]}'}</td><td><b>{r['adds']}</b></td></tr>"
    if not products_html:
        products_html = "<tr><td colspan='3' style='color:#aaa'>Andmed puuduvad</td></tr>"

    # Päevane aktiivsus tabel
    daily_dict = {}
    for r in daily:
        d = str(r['day'])
        if d not in daily_dict:
            daily_dict[d] = {}
        daily_dict[d][r['event_type']] = r['cnt']

    daily_html = ""
    for day in sorted(daily_dict.keys(), reverse=True):
        data = daily_dict[day]
        daily_html += f"""<tr>
            <td>{day}</td>
            <td>{data.get('basket_add', 0)}</td>
            <td>{data.get('basket_win', 0)}</td>
            <td>{data.get('product_view', 0)}</td>
        </tr>"""
    if not daily_html:
        daily_html = "<tr><td colspan='4' style='color:#aaa'>Andmed puuduvad</td></tr>"

    title = f"Analytics — {chain.capitalize() if chain else 'Kõik ketid'} ({days}p)"

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Seivy Analytics</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #f5f5f5; margin: 0; padding: 16px; color: #222; }}
  h1 {{ color: #FF9100; margin-bottom: 4px; }}
  h2 {{ font-size: 1rem; color: #555; margin: 24px 0 8px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 16px; }}
  .card {{ background: white; border-radius: 10px; padding: 16px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  .card .num {{ font-size: 2rem; font-weight: 700; color: #FF9100; }}
  .card .label {{ font-size: 0.8rem; color: #888; margin-top: 2px; }}
  table {{ width: 100%; background: white; border-radius: 10px;
           box-shadow: 0 1px 4px rgba(0,0,0,0.08); border-collapse: collapse; margin-bottom: 16px; }}
  th {{ background: #f0f0f0; padding: 8px 12px; text-align: left; font-size: 0.8rem; color: #555; }}
  td {{ padding: 8px 12px; border-top: 1px solid #f0f0f0; font-size: 0.9rem; }}
  .back {{ color: #FF9100; text-decoration: none; font-size: 0.9rem; }}
  .filters {{ margin-bottom: 16px; }}
</style>
</head><body>
<a class="back" href="/">← Admin</a>
<h1>📊 {title}</h1>

<div class="filters">
  <div style="margin-bottom:8px"><b>Kett:</b><br>{chain_btns}</div>
  <div><b>Periood:</b><br>{days_btns}</div>
</div>

<h2>Kokkuvõte — viimased {days} päeva</h2>
<div class="grid">
  <div class="card"><div class="num">{totals['total_adds'] or 0}</div><div class="label">Korvi lisamised</div></div>
  <div class="card"><div class="num">{totals['total_wins'] or 0}</div><div class="label">Korvi võidud</div></div>
  <div class="card"><div class="num">{totals['total_views'] or 0}</div><div class="label">Toote vaatamised</div></div>
  <div class="card"><div class="num">{totals['unique_users'] or 0}</div><div class="label">Unikaalsed kasutajad</div></div>
</div>

<h2>Kes võidab korvi?</h2>
<table>
  <tr><th>Kett</th><th>Võidud</th><th>Osakaal</th></tr>
  {wins_html}
</table>

<h2>Populaarseimad tooted korvis</h2>
<table>
  <tr><th>#</th><th>Toode</th><th>Korvi lisamisi</th></tr>
  {products_html}
</table>

<h2>Päevane aktiivsus (viimased 14 päeva)</h2>
<table>
  <tr><th>Kuupäev</th><th>Korvi lisamised</th><th>Korvi võidud</th><th>Toote vaatamised</th></tr>
  {daily_html}
</table>

<p style="color:#aaa;font-size:0.75rem;text-align:center">Seivy Analytics · {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}</p>
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
