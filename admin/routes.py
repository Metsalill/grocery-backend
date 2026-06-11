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

    _admin_tok = os.environ.get('ANALYTICS_TOKEN_ADMIN', '')
    _analytics_href = f'/admin/analytics?token={_admin_tok}' if _admin_tok else '#'

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
<a class="analytics-link" href="{_analytics_href}">📊 Vaata Analytics Dashboardi</a>

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


@router.get("/admin/analytics", response_class=HTMLResponse)
async def analytics_dashboard(request: Request, token: str = None, days: int = 30, chain: str = None):
    import json, os
    from html import escape
    from urllib.parse import urlencode

    # Token -> chain mapping
    TOKEN_MAP = {
        os.environ.get("ANALYTICS_TOKEN_SELVER", ""): "selver",
        os.environ.get("ANALYTICS_TOKEN_RIMI", ""): "rimi",
        os.environ.get("ANALYTICS_TOKEN_PRISMA", ""): "prisma",
        os.environ.get("ANALYTICS_TOKEN_COOP", ""): "coop",
        os.environ.get("ANALYTICS_TOKEN_MAXIMA", ""): "maxima",
    }
    TOKEN_MAP.pop("", None)  # eemalda tühi võti kui env puudub

    # Admin token annab kõigi kettide vaate
    admin_token = os.environ.get("ANALYTICS_TOKEN_ADMIN", "")

    if not token:
        return HTMLResponse("<h2>Ligipääs keelatud. Token puudub.</h2>", status_code=403)

    if admin_token and token == admin_token:
        pass  # admin — chain tuleb URL-ist (võib olla None = kõik ketid)
    elif token in TOKEN_MAP:
        chain = TOKEN_MAP[token]  # keti token lukustab chain
    else:
        return HTMLResponse("<h2>Ligipääs keelatud. Token on vale.</h2>", status_code=403)

    if getattr(request.app.state, "db", None) is None:
        return HTMLResponse("<h2>DB not ready yet.</h2>", status_code=503)

    async with request.app.state.db.acquire() as conn:
        basket_wins_rows = await conn.fetch("""
            SELECT chain, COUNT(*) AS wins
            FROM analytics_events
            WHERE event_type = 'basket_win'
              AND created_at >= NOW() - ($1 || ' days')::INTERVAL
              AND ($2::text IS NULL OR LOWER(chain) = LOWER($2))
            GROUP BY chain
            ORDER BY wins DESC
        """, str(days), chain)

        top_products_rows = await conn.fetch("""
            SELECT
                a.product_id,
                p.name,
                COUNT(*) AS adds
            FROM analytics_events a
            LEFT JOIN products p ON p.id = a.product_id
            WHERE a.event_type = 'basket_add'
              AND a.product_id IS NOT NULL
              AND a.created_at >= NOW() - ($1 || ' days')::INTERVAL
              AND ($2::text IS NULL OR LOWER(a.chain) = LOWER($2))
            GROUP BY a.product_id, p.name
            ORDER BY adds DESC
            LIMIT 10
        """, str(days), chain)

        daily_rows = await conn.fetch("""
            SELECT
                DATE(created_at) AS day,
                event_type,
                COUNT(*) AS cnt
            FROM analytics_events
            WHERE created_at >= NOW() - INTERVAL '14 days'
              AND ($1::text IS NULL OR LOWER(chain) = LOWER($1))
            GROUP BY DATE(created_at), event_type
            ORDER BY day ASC
        """, chain)

        totals = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'basket_add') AS total_adds,
                COUNT(*) FILTER (WHERE event_type = 'basket_win') AS total_wins,
                COUNT(*) FILTER (WHERE event_type = 'product_view') AS total_views,
                COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL) AS unique_users
            FROM analytics_events
            WHERE created_at >= NOW() - ($1 || ' days')::INTERVAL
              AND ($2::text IS NULL OR LOWER(chain) = LOWER($2))
        """, str(days), chain)

    # Kas admin token?
    is_admin = admin_token and token == admin_token
    chain_filter_name = chain.capitalize() if chain else "Kõik ketid"

    # Chain filter — ainult adminile
    CHAINS = [("", "Kõik"), ("selver", "Selver"), ("rimi", "Rimi"), ("prisma", "Prisma"), ("coop", "Coop"), ("maxima", "Maxima")]
    if is_admin:
        chain_btns = "".join(
            f'<a class="filter-pill{"  active" if (not chain and not value) or value == chain else ""}" href="/admin/analytics?token={escape(token)}&days={days}{"&chain=" + value if value else ""}">{escape(label)}</a>'
            for value, label in CHAINS
        )
    else:
        chain_btns = ""  # ketil pole chain filtrit

    # Days filter pills — token säilib URL-is
    chain_param = f"&chain={chain}" if chain else ""
    days_btns = "".join(
        f'<a class="filter-pill{"  active" if period == days else ""}" href="/admin/analytics?token={escape(token)}&days={period}{chain_param}">{period}p</a>'
        for period in [7, 14, 30, 90]
    )

    # Wins HTML
    max_wins = max((r['wins'] for r in basket_wins_rows), default=0)
    wins_html = "".join(
        f"""<div class="ranking-item">
            <div class="ranking-row">
                <div class="ranking-name"><span class="ranking-position">{i}</span><span>{escape(r['chain'] or '—')}</span></div>
                <span class="ranking-count">{r['wins']:,}<small> võitu</small></span>
            </div>
            <div class="progress-track"><span class="progress-fill" style="--bar-width:{round(r['wins']/max_wins*100,1) if max_wins else 0}%"></span></div>
        </div>"""
        for i, r in enumerate(basket_wins_rows, 1)
    ) or '<div class="empty-state">Valitud perioodi kohta ei ole veel korvi võitude andmeid.</div>'

    # Products HTML
    max_adds = max((r['adds'] for r in top_products_rows), default=0)
    products_html = "".join(
        f"""<div class="product-row">
            <span class="product-rank">{i}</span>
            <div class="product-content">
                <div class="product-name" title="{escape(r['name'] or '')}">{escape(r['name'] or f'ID {r["product_id"]}')}</div>
                <div class="product-bar-track"><span class="product-bar-fill" style="--bar-width:{round(r['adds']/max_adds*100,1) if max_adds else 0}%"></span></div>
            </div>
            <div class="product-count">{r['adds']:,}<small>lisamist</small></div>
        </div>"""
        for i, r in enumerate(top_products_rows, 1)
    ) or '<div class="empty-state">Valitud perioodi kohta ei ole veel toodete lisamise andmeid.</div>'

    # Daily chart data
    daily_dict = {}
    for r in daily_rows:
        d = str(r['day'])
        if d not in daily_dict:
            daily_dict[d] = {}
        daily_dict[d][r['event_type']] = r['cnt']

    sorted_days = sorted(daily_dict.keys())
    daily_labels_json = json.dumps([d[5:] for d in sorted_days], ensure_ascii=False)
    daily_adds_json = json.dumps([daily_dict[d].get('basket_add', 0) for d in sorted_days])
    daily_wins_json = json.dumps([daily_dict[d].get('basket_win', 0) for d in sorted_days])

    title = f"{chain.capitalize() if chain else 'Kõik ketid'} — viimased {days} päeva"
    total_adds = totals['total_adds'] or 0
    total_wins = totals['total_wins'] or 0
    total_views = totals['total_views'] or 0
    unique_users = totals['unique_users'] or 0
    chain_filter_label = chain.capitalize() if chain else "Kõik ketid"

    html = f"""<!DOCTYPE html>
<html lang="et">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="light">
<title>Seivy Analytics</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root {{
    --accent: #FF9100;
    --accent-dark: #E97800;
    --accent-soft: #FFF4E5;
    --accent-softer: #FFF9F2;
    --background: #F5F6F8;
    --surface: #FFFFFF;
    --surface-muted: #F8F9FB;
    --text: #171A1F;
    --text-secondary: #68707D;
    --text-muted: #9198A3;
    --border: #E6E9EE;
    --border-strong: #D9DDE4;
    --success: #1B9A59;
    --success-soft: #EAF8F0;
    --shadow: 0 1px 2px rgba(20,24,32,.04), 0 8px 24px rgba(20,24,32,.055);
    --radius-lg: 20px;
    --radius-md: 14px;
    --radius-sm: 10px;
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--background); color: var(--text); font-family: Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; -webkit-font-smoothing: antialiased; }}
button, a {{ font: inherit; }}
.topbar {{ position: sticky; top: 0; z-index: 50; border-bottom: 1px solid rgba(230,233,238,.92); background: rgba(255,255,255,.92); backdrop-filter: blur(16px); }}
.topbar-inner {{ display: flex; align-items: center; justify-content: space-between; width: min(1440px, calc(100% - 48px)); min-height: 72px; margin: 0 auto; gap: 24px; }}
.brand {{ display: flex; align-items: center; gap: 12px; }}
.brand-mark {{ display: grid; width: 40px; height: 40px; place-items: center; border-radius: 12px; background: var(--accent); color: #fff; font-size: 19px; font-weight: 800; letter-spacing: -.03em; }}
.brand-name {{ margin: 0; font-size: 18px; font-weight: 750; letter-spacing: -.03em; }}
.brand-section {{ margin: 2px 0 0; color: var(--text-secondary); font-size: 12px; font-weight: 550; }}
.live-dot {{ width: 8px; height: 8px; border-radius: 999px; background: var(--success); box-shadow: 0 0 0 4px var(--success-soft); }}
.topbar-context {{ display: flex; align-items: center; gap: 10px; color: var(--text-secondary); font-size: 13px; font-weight: 600; }}
.content {{ width: min(1440px, calc(100% - 48px)); margin: 0 auto; padding: 38px 0 56px; }}
.page-heading {{ display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 28px; gap: 24px; }}
.eyebrow {{ display: inline-flex; align-items: center; margin-bottom: 10px; gap: 8px; color: var(--accent-dark); font-size: 12px; font-weight: 750; letter-spacing: .08em; text-transform: uppercase; }}
.eyebrow::before {{ width: 18px; height: 2px; border-radius: 999px; background: var(--accent); content: ""; }}
h1 {{ margin: 0; font-size: clamp(28px,3vw,42px); font-weight: 760; letter-spacing: -.045em; line-height: 1.08; }}
.heading-description {{ max-width: 690px; margin: 12px 0 0; color: var(--text-secondary); font-size: 15px; line-height: 1.65; }}
.period-summary {{ flex: 0 0 auto; padding: 12px 16px; border: 1px solid var(--border); border-radius: var(--radius-md); background: var(--surface); font-size: 13px; font-weight: 650; }}
.period-summary span {{ display: block; margin-bottom: 3px; color: var(--text-secondary); font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .05em; }}
.period-summary strong {{ display: block; color: var(--text); font-size: 14px; }}
.filters-panel {{ display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 22px; padding: 18px 20px; border: 1px solid var(--border); border-radius: var(--radius-lg); background: var(--surface); gap: 24px; }}
.filter-groups {{ display: flex; flex-wrap: wrap; gap: 26px; }}
.filter-group {{ display: flex; flex-direction: column; gap: 9px; }}
.filter-label {{ color: var(--text-muted); font-size: 11px; font-weight: 750; letter-spacing: .07em; text-transform: uppercase; }}
.filter-pills {{ display: flex; flex-wrap: wrap; gap: 7px; }}
.filter-pill {{ display: inline-flex; align-items: center; justify-content: center; min-height: 36px; padding: 8px 14px; border: 1px solid var(--border); border-radius: 999px; background: var(--surface); color: var(--text-secondary); font-size: 13px; font-weight: 650; text-decoration: none; transition: border-color 160ms,background 160ms,color 160ms; }}
.filter-pill:hover {{ border-color: #FFC06C; background: var(--accent-softer); color: var(--accent-dark); }}
.filter-pill.active {{ border-color: var(--accent); background: var(--accent); color: #fff; box-shadow: 0 5px 12px rgba(255,145,0,.18); }}
.filter-context {{ flex: 0 0 auto; color: var(--text-secondary); font-size: 13px; text-align: right; }}
.filter-context strong {{ display: block; margin-top: 3px; color: var(--text); font-size: 14px; }}
.metrics-grid {{ display: grid; grid-template-columns: repeat(4,minmax(0,1fr)); margin-bottom: 22px; gap: 16px; }}
.metric-card {{ position: relative; overflow: hidden; padding: 22px; border: 1px solid var(--border); border-radius: var(--radius-lg); background: var(--surface); box-shadow: var(--shadow); }}
.metric-card::after {{ position: absolute; top: -48px; right: -48px; width: 116px; height: 116px; border-radius: 999px; background: var(--accent-soft); content: ""; opacity: .65; }}
.metric-top {{ position: relative; z-index: 1; display: flex; align-items: center; justify-content: space-between; margin-bottom: 19px; gap: 16px; }}
.metric-label {{ color: var(--text-secondary); font-size: 13px; font-weight: 650; }}
.metric-icon {{ display: grid; width: 38px; height: 38px; place-items: center; border-radius: 11px; background: var(--accent-soft); color: var(--accent-dark); }}
.metric-icon svg {{ width: 19px; height: 19px; stroke: currentColor; }}
.metric-value {{ position: relative; z-index: 1; margin: 0; font-size: clamp(27px,2.4vw,38px); font-weight: 770; letter-spacing: -.045em; line-height: 1; }}
.metric-note {{ position: relative; z-index: 1; margin: 10px 0 0; color: var(--text-muted); font-size: 12px; font-weight: 550; }}
.dashboard-grid {{ display: grid; grid-template-columns: minmax(0,1.6fr) minmax(330px,.8fr); gap: 22px; }}
.dashboard-column {{ display: flex; flex-direction: column; gap: 22px; }}
.panel {{ padding: 24px; border: 1px solid var(--border); border-radius: var(--radius-lg); background: var(--surface); box-shadow: var(--shadow); }}
.panel-header {{ display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 23px; gap: 18px; }}
.panel-title {{ margin: 0; color: var(--text); font-size: 17px; font-weight: 730; letter-spacing: -.025em; }}
.panel-description {{ margin: 6px 0 0; color: var(--text-secondary); font-size: 12px; line-height: 1.5; }}
.panel-badge {{ padding: 7px 10px; border-radius: 999px; background: var(--surface-muted); color: var(--text-secondary); font-size: 11px; font-weight: 700; }}
.chart-wrapper {{ position: relative; width: 100%; height: 355px; }}
.chart-wrapper.compact {{ height: 320px; }}
.ranking-list {{ display: flex; flex-direction: column; gap: 11px; }}
.ranking-item {{ padding: 14px; border: 1px solid var(--border); border-radius: var(--radius-md); background: var(--surface-muted); }}
.ranking-row {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; gap: 12px; }}
.ranking-name {{ display: flex; align-items: center; gap: 10px; color: var(--text); font-size: 13px; font-weight: 680; }}
.ranking-position {{ display: grid; width: 26px; height: 26px; place-items: center; border-radius: 8px; background: #fff; color: var(--text-secondary); font-size: 11px; font-weight: 800; box-shadow: 0 1px 3px rgba(20,24,32,.07); }}
.ranking-item:first-child .ranking-position {{ background: var(--accent); color: #fff; }}
.ranking-count {{ color: var(--text); font-size: 13px; font-weight: 750; }}
.ranking-count small {{ color: var(--text-muted); font-size: 10px; font-weight: 600; }}
.progress-track {{ width: 100%; height: 7px; overflow: hidden; border-radius: 999px; background: #E8EBF0; }}
.progress-fill {{ display: block; width: var(--bar-width,0%); height: 100%; border-radius: inherit; background: linear-gradient(90deg,var(--accent),#FFB343); }}
.product-list {{ display: flex; flex-direction: column; gap: 2px; }}
.product-row {{ display: grid; grid-template-columns: 32px minmax(0,1fr) auto; align-items: center; padding: 13px 4px; border-bottom: 1px solid var(--border); gap: 12px; }}
.product-row:last-child {{ border-bottom: 0; }}
.product-rank {{ display: grid; width: 28px; height: 28px; place-items: center; border-radius: 8px; background: var(--surface-muted); color: var(--text-secondary); font-size: 11px; font-weight: 750; }}
.product-row:first-child .product-rank {{ background: var(--accent-soft); color: var(--accent-dark); }}
.product-name {{ margin-bottom: 8px; overflow: hidden; color: var(--text); font-size: 13px; font-weight: 650; line-height: 1.35; text-overflow: ellipsis; white-space: nowrap; }}
.product-bar-track {{ width: 100%; height: 5px; overflow: hidden; border-radius: 999px; background: #ECEFF3; }}
.product-bar-fill {{ display: block; width: var(--bar-width,0%); height: 100%; border-radius: inherit; background: var(--accent); }}
.product-count {{ min-width: 58px; color: var(--text); font-size: 13px; font-weight: 750; text-align: right; }}
.product-count small {{ display: block; margin-top: 2px; color: var(--text-muted); font-size: 9px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }}
.empty-state {{ display: grid; min-height: 180px; place-items: center; padding: 24px; border: 1px dashed var(--border-strong); border-radius: var(--radius-md); background: var(--surface-muted); color: var(--text-secondary); font-size: 13px; text-align: center; }}
.footer {{ display: flex; align-items: center; justify-content: space-between; margin-top: 24px; padding: 0 4px; gap: 20px; color: var(--text-muted); font-size: 11px; }}
.footer-brand {{ color: var(--text-secondary); font-weight: 700; }}
@media (max-width: 1100px) {{
    .metrics-grid {{ grid-template-columns: repeat(2,minmax(0,1fr)); }}
    .dashboard-grid {{ grid-template-columns: 1fr; }}
}}
@media (max-width: 760px) {{
    .topbar-inner, .content {{ width: min(100% - 28px, 1440px); }}
    .metrics-grid {{ grid-template-columns: 1fr; }}
    .filters-panel {{ flex-direction: column; }}
}}
</style>
</head>
<body>
<header class="topbar">
  <div class="topbar-inner">
    <div class="brand">
      <div class="brand-mark">S</div>
      <div>
        <p class="brand-name">Seivy</p>
        <p class="brand-section">Partneranalüütika</p>
      </div>
    </div>
    <div class="topbar-context">
      <span class="live-dot"></span>
      Andmed on aktiivsed
    </div>
  </div>
</header>

<main class="content">
  <section class="page-heading">
    <div>
      <div class="eyebrow">Jaeketi ülevaade</div>
      <h1>Jaekettide tulemuslikkus</h1>
  <p style="margin:6px 0 0;color:var(--text-secondary);font-size:16px;font-weight:500">{title}</p>
      <p class="heading-description">{'Ülevaade sellest, kuidas kasutajad ' + chain.capitalize() + ' tooteid vaatavad, ostukorvi lisavad ja millistes hinnavõrdlustes saavutab kett soodsaima ostukorvi tulemuse.' if chain else 'Ülevaade sellest, kuidas kasutajad tooteid vaatavad, ostukorvi lisavad ja millised jaeketid saavutavad hinnavõrdlustes soodsaima ostukorvi tulemuse.'}</p>
    </div>
    <div class="period-summary"><span>Analüüsiperiood</span><strong>Viimased {days} päeva</strong></div>
  </section>

  <section class="filters-panel">
    <div class="filter-groups">
      <div class="filter-group">
        <div class="filter-label">Jaekett</div>
        <nav class="filter-pills">{chain_btns}</nav>
      </div>
      <div class="filter-group">
        <div class="filter-label">Periood</div>
        <nav class="filter-pills">{days_btns}</nav>
      </div>
    </div>
    <div class="filter-context">Hetkel kuvatakse<strong>{chain_filter_label}</strong></div>
  </section>

  <section class="metrics-grid">
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Korvi lisamised</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 4h2l2.3 10.2a2 2 0 0 0 2 1.6h7.7a2 2 0 0 0 1.9-1.4L21 8H7"></path><path d="M12 6v6"></path><path d="M9 9h6"></path><circle cx="10" cy="20" r="1"></circle><circle cx="18" cy="20" r="1"></circle></svg></span>
      </div>
      <p class="metric-value">{total_adds:,}</p>
      <p class="metric-note">Toodete lisamised kasutajate korvidesse</p>
    </article>
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Korvi võidud</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3h8v4a4 4 0 0 1-8 0V3Z"></path><path d="M6 5H3v1a5 5 0 0 0 5 5"></path><path d="M18 5h3v1a5 5 0 0 1-5 5"></path><path d="M12 11v5"></path><path d="M8 21h8"></path><path d="M10 16h4v5h-4z"></path></svg></span>
      </div>
      <p class="metric-value">{total_wins:,}</p>
      <p class="metric-note">Soodsaima ostukorvi tulemused</p>
    </article>
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Toote vaatamised</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z"></path><circle cx="12" cy="12" r="2.5"></circle></svg></span>
      </div>
      <p class="metric-value">{total_views:,}</p>
      <p class="metric-note">Tootekaartide ja detailvaadete avamised</p>
    </article>
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Unikaalsed kasutajad</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="8" r="3"></circle><path d="M3 20a6 6 0 0 1 12 0"></path><circle cx="17" cy="9" r="2"></circle><path d="M15.5 15.5A5 5 0 0 1 21 20"></path></svg></span>
      </div>
      <p class="metric-value">{unique_users:,}</p>
      <p class="metric-note">Aktiivsed kasutajad valitud perioodil</p>
    </article>
  </section>

  <section class="dashboard-grid">
    <div class="dashboard-column">
      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Igapäevane aktiivsus</h2>
            <p class="panel-description">Korvi lisamiste ja soodsaima korvi tulemuste muutus päevade lõikes.</p>
          </div>
          <span class="panel-badge">{days} päeva</span>
        </div>
        <div class="chart-wrapper">
          <canvas id="dailyActivityChart" aria-label="Igapäevase aktiivsuse tulpdiagramm"></canvas>
        </div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Enim korvi lisatud tooted</h2>
            <p class="panel-description">Tooted, mille vastu on kasutajad kõige suuremat ostuhuvi näidanud.</p>
          </div>
          <span class="panel-badge">Top 10</span>
        </div>
        <div class="product-list">{products_html}</div>
      </article>
    </div>

    <div class="dashboard-column">
      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Soodsaima korvi positsioon</h2>
            <p class="panel-description">Kui sageli saavutas kett võrreldud ostukorvide seas madalaima koguhinna.</p>
          </div>
        </div>
        <div class="ranking-list">{wins_html}</div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Aktiivsuse trend</h2>
            <p class="panel-description">Korvi lisamised ja võidud samal ajaskaalal (kuvab vähemalt 2 päeva andmetega).</p>
          </div>
        </div>
        <div class="chart-wrapper compact">
          <canvas id="activityOverviewChart" aria-label="Aktiivsuse trendijoonis"></canvas>
        </div>
      </article>
    </div>
  </section>

  <footer class="footer">
    <span>Näitajad põhinevad Seivy rakenduses anonüümselt kogutud kasutussündmustel.</span>
    <span class="footer-brand">Seivy partneranalüütika · <a href="/" style="color:inherit">← Admin</a></span>
  </footer>
</main>

<script>
(function() {{
  "use strict";
  const labels = {daily_labels_json};
  const dailyAdds = {daily_adds_json};
  const dailyWins = {daily_wins_json};

  Chart.defaults.font.family = 'Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif';
  Chart.defaults.color = "#68707D";
  Chart.defaults.borderColor = "#E9ECF1";

  const sharedScales = {{
    x: {{ grid: {{ display: false }}, border: {{ display: false }}, ticks: {{ color: "#7A828E", font: {{ size: 11, weight: "600" }}, maxRotation: 0, autoSkip: true, maxTicksLimit: 8 }} }},
    y: {{ beginAtZero: true, grid: {{ color: "#EEF0F4", drawTicks: false }}, border: {{ display: false }}, ticks: {{ color: "#8B929D", padding: 10, precision: 0, font: {{ size: 10, weight: "600" }} }} }}
  }};

  const tooltipOpts = {{
    backgroundColor: "#171A1F", titleColor: "#fff", bodyColor: "#fff",
    padding: 12, cornerRadius: 10, displayColors: true, boxPadding: 5
  }};

  const barCanvas = document.getElementById("dailyActivityChart");
  if (barCanvas) {{
    new Chart(barCanvas, {{
      type: "bar",
      data: {{
        labels,
        datasets: [
          {{ label: "Korvi lisamised", data: dailyAdds, backgroundColor: "#FF9100", borderRadius: 6, maxBarThickness: 26 }},
          {{ label: "Korvi võidud", data: dailyWins, backgroundColor: "#FFD7A3", borderRadius: 6, maxBarThickness: 26 }}
        ]
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        interaction: {{ mode: "index", intersect: false }},
        plugins: {{
          legend: {{ position: "top", align: "end", labels: {{ usePointStyle: true, pointStyle: "rectRounded", boxWidth: 8, boxHeight: 8, padding: 18, font: {{ size: 11, weight: "650" }} }} }},
          tooltip: tooltipOpts
        }},
        scales: sharedScales
      }}
    }});
  }}

  const lineCanvas = document.getElementById("activityOverviewChart");
  if (lineCanvas && labels.length >= 2) {{
    new Chart(lineCanvas, {{
      type: "line",
      data: {{
        labels,
        datasets: [
          {{ label: "Korvi lisamised", data: dailyAdds, borderColor: "#FF9100", backgroundColor: "rgba(255,145,0,.15)", fill: true, borderWidth: 2.5, pointRadius: 0, pointHoverRadius: 4, tension: 0.35 }},
          {{ label: "Korvi võidud", data: dailyWins, borderColor: "#3977F6", backgroundColor: "transparent", fill: false, borderWidth: 2, pointRadius: 0, pointHoverRadius: 4, tension: 0.35 }}
        ]
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        interaction: {{ mode: "index", intersect: false }},
        plugins: {{
          legend: {{ position: "top", align: "start", labels: {{ usePointStyle: true, pointStyle: "circle", boxWidth: 7, boxHeight: 7, padding: 16, font: {{ size: 11, weight: "650" }} }} }},
          tooltip: tooltipOpts
        }},
        scales: sharedScales
      }}
    }});
  }} else if (lineCanvas) {{
    lineCanvas.parentElement.innerHTML = '<div class="empty-state">Trendi kuvamiseks on vaja vähemalt kahe päeva andmeid.</div>';
  }}
}})();
</script>
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
