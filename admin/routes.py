# admin/routes.py
import os, shutil, datetime
from fastapi import APIRouter, Depends, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from settings import IMAGES_DIR, MAX_UPLOAD_MB, CDN_BASE_URL
from .security import basic_guard

router = APIRouter()


@router.get("/", response_class=HTMLResponse, dependencies=[Depends(basic_guard)])
async def dashboard(request: Request):
    if getattr(request.app.state, "db", None) is None:
        return HTMLResponse("<h2>DB not ready yet. Try again in a few seconds.</h2>", status_code=503)

    async with request.app.state.db.acquire() as conn:
        users = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') AS today,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') AS week
            FROM users WHERE deleted_at IS NULL
        """)

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

        no_price = await conn.fetch("""
            SELECT p.sub_code, COUNT(DISTINCT p.id) AS cnt
            FROM products p
            WHERE NOT EXISTS (SELECT 1 FROM prices pr WHERE pr.product_id = p.id)
            AND p.sub_code IS NOT NULL
            GROUP BY p.sub_code
            ORDER BY cnt DESC
            LIMIT 5
        """)

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

        scraper_rows = await conn.fetch("""
            SELECT
                chain,
                MAX(last_seen_utc) AS last_update,
                COUNT(*) FILTER (WHERE last_seen_utc >= NOW() - INTERVAL '24 hours') AS updated_today
            FROM products
            GROUP BY chain
            ORDER BY last_update DESC NULLS LAST
        """)

        null_subcode = await conn.fetchval("SELECT COUNT(*) FROM products WHERE sub_code IS NULL")

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
  .analytics-link {{ display: inline-block; margin-top: 8px; margin-right: 8px; padding: 8px 16px;
                     background: #FF9100; color: white; border-radius: 8px;
                     text-decoration: none; font-weight: 600; font-size: 0.9rem; }}
  .partners-link {{ background: #1A1A1A; }}
  @media (max-width: 480px) {{
    .analytics-link {{ display: block; margin-right: 0; text-align: center; }}
  }}
</style>
</head><body>
<h1>Seivy Admin</h1>
<a class="analytics-link" href="{_analytics_href}">📊 Vaata Analytics Dashboardi</a>
<a class="analytics-link partners-link" href="/admin/partners">🏷️ Halda partnereid (tootjad/ketid)</a>

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

<h2>Scraperitе aktiivsus</h2>
<table>
  <tr><th>Kett</th><th>Viimane update</th><th>Täna uuendatud</th></tr>
  {scraper_html}
</table>

<h2>Kettide coverage</h2>
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


async def _render_brand_dashboard(conn, partner_name: str, brand_filter: list, days: int):
    """Renders the brand/producer analytics view. Separate from the
    retailer/admin dashboard function to keep the two code paths
    independent — a bug in one cannot affect the other. Reuses the same
    CSS design system (colors, radii, card styles) per the design brief:
    same accent color, just different heading/KPI semantics and a
    'Tootja' badge, per ChatGPT design consultation.
    """
    import json
    from html import escape

    ALLOWED_DAYS = {7, 14, 30, 90}
    if days not in ALLOWED_DAYS:
        days = 30

    brand_filter_lower = [b.lower() for b in brand_filter]

    # Resolve all product_ids belonging to groups whose canonical brand
    # matches this partner's brand_filter (case-insensitive exact match
    # only — see design decision: no ILIKE wildcard, to avoid a partner
    # ever seeing a competitor's products due to a fuzzy match).
    product_id_rows = await conn.fetch("""
        SELECT pgm.product_id, pgm.group_id
        FROM product_group_members pgm
        JOIN product_groups pg ON pg.id = pgm.group_id
        WHERE LOWER(pg.brand) = ANY($1::text[])
    """, brand_filter_lower)
    brand_product_ids = [r["product_id"] for r in product_id_rows]
    brand_group_ids = list({r["group_id"] for r in product_id_rows})

    if not brand_product_ids:
        return HTMLResponse(
            f"<h2>{escape(partner_name)}: brändi tooteid ei leitud. "
            "Kontrolli brand_filter väärtust /admin/partners lehel.</h2>",
            status_code=200,
        )

    # "Viimane sündmus" peab kajastama BRÄNDI enda toodete viimast
    # sündmust, mitte kogu Seivy süsteemi viimast sündmust — muidu näeks
    # partner eksitavalt värsket ajatemplit ka siis, kui tema enda
    # toodetel pole päevi/nädalaid tegevust olnud.
    last_event = await conn.fetchval("""
        SELECT MAX(created_at) FROM analytics_events
        WHERE product_id = ANY($1::int[])
    """, brand_product_ids)
    if last_event:
        diff = datetime.datetime.now(datetime.timezone.utc) - last_event
        mins = int(diff.total_seconds() / 60)
        if mins < 60:
            last_event_str = f"{mins} min tagasi"
        elif mins < 1440:
            last_event_str = f"{mins // 60} tundi tagasi"
        else:
            last_event_str = last_event.strftime("%d.%m kell %H:%M")
    else:
        last_event_str = "Andmed puuduvad"

    totals = await conn.fetchrow("""
        SELECT
            COUNT(*) FILTER (WHERE event_type = 'basket_add') AS total_adds,
            COUNT(*) FILTER (WHERE event_type = 'product_view') AS total_views,
            COUNT(DISTINCT COALESCE(
                CASE WHEN user_id IS NOT NULL THEN 'u:' || user_id::text END,
                CASE WHEN device_key IS NOT NULL AND device_key <> '' THEN 'd:' || device_key END
            )) AS unique_visitors
        FROM analytics_events
        WHERE product_id = ANY($1::int[])
          AND created_at >= CURRENT_DATE - ($2::int - 1)
          AND created_at < CURRENT_DATE + INTERVAL '1 day'
    """, brand_product_ids, days)

    prev_totals = await conn.fetchrow("""
        SELECT
            COUNT(*) FILTER (WHERE event_type = 'basket_add') AS total_adds,
            COUNT(*) FILTER (WHERE event_type = 'product_view') AS total_views,
            COUNT(DISTINCT COALESCE(
                CASE WHEN user_id IS NOT NULL THEN 'u:' || user_id::text END,
                CASE WHEN device_key IS NOT NULL AND device_key <> '' THEN 'd:' || device_key END
            )) AS unique_visitors
        FROM analytics_events
        WHERE product_id = ANY($1::int[])
          AND created_at >= CURRENT_DATE - (($2::int * 2) - 1)
          AND created_at < CURRENT_DATE - ($2::int - 1)
    """, brand_product_ids, days)

    top_products_rows = await conn.fetch("""
        SELECT a.product_id, p.name, COUNT(*) AS adds
        FROM analytics_events a
        JOIN products p ON p.id = a.product_id
        WHERE a.event_type = 'basket_add'
          AND a.product_id = ANY($1::int[])
          AND a.created_at >= CURRENT_DATE - ($2::int - 1)
          AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
        GROUP BY a.product_id, p.name
        ORDER BY adds DESC
        LIMIT 10
    """, brand_product_ids, days)

    daily_rows = await conn.fetch("""
        SELECT DATE(created_at) AS day, event_type, COUNT(*) AS cnt
        FROM analytics_events
        WHERE product_id = ANY($1::int[])
          AND created_at >= CURRENT_DATE - ($2::int - 1)
          AND created_at < CURRENT_DATE + INTERVAL '1 day'
        GROUP BY DATE(created_at), event_type
        ORDER BY day ASC
    """, brand_product_ids, days)

    # Price comparison heat-map: up to 15 groups, each row = one grouped
    # product, columns = the 5 known chains. Limited to groups with the
    # most analytics demand in the period, so the table shows the
    # partner's most-relevant products first.
    price_rows = await conn.fetch("""
        WITH demand AS (
            SELECT pgm.group_id, COUNT(*) AS demand_count
            FROM analytics_events a
            JOIN product_group_members pgm ON pgm.product_id = a.product_id
            WHERE pgm.group_id = ANY($1::int[])
              AND a.event_type IN ('product_view', 'basket_add')
              AND a.created_at >= CURRENT_DATE - ($2::int - 1)
              AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
            GROUP BY pgm.group_id
        ),
        top_groups AS (
            SELECT group_id FROM demand ORDER BY demand_count DESC LIMIT 15
        )
        SELECT
            pg.id AS group_id,
            COALESCE(pg.canonical_name, 'Toode #' || pg.id) AS name,
            LOWER(s.chain) AS chain,
            MIN(COALESCE(NULLIF(pr.promo_price, 0), pr.price)) AS price
        FROM top_groups tg
        JOIN product_groups pg ON pg.id = tg.group_id
        JOIN product_group_members pgm ON pgm.group_id = pg.id
        JOIN prices pr ON pr.product_id = pgm.product_id
        JOIN stores s ON s.id = pr.store_id
        WHERE pr.collected_at > NOW() - INTERVAL '14 days'
          AND pr.price > 0
        GROUP BY pg.id, pg.canonical_name, LOWER(s.chain)
    """, brand_group_ids, days)

    # If no analytics-driven demand yet (e.g. brand new partner), fall
    # back to just showing any 15 groups for this brand so the table
    # isn't empty on day one.
    if not price_rows and brand_group_ids:
        price_rows = await conn.fetch("""
            WITH top_groups AS (
                SELECT id AS group_id
                FROM product_groups
                WHERE id = ANY($1::int[])
                ORDER BY canonical_name ASC
                LIMIT 15
            )
            SELECT
                pg.id AS group_id,
                COALESCE(pg.canonical_name, 'Toode #' || pg.id) AS name,
                LOWER(s.chain) AS chain,
                MIN(COALESCE(NULLIF(pr.promo_price, 0), pr.price)) AS price
            FROM top_groups tg
            JOIN product_groups pg ON pg.id = tg.group_id
            JOIN product_group_members pgm ON pgm.group_id = pg.id
            JOIN prices pr ON pr.product_id = pgm.product_id
            JOIN stores s ON s.id = pr.store_id
            WHERE pr.collected_at > NOW() - INTERVAL '14 days'
              AND pr.price > 0
            GROUP BY pg.id, pg.canonical_name, LOWER(s.chain)
        """, brand_group_ids)

    # Pivot into {group_id: {name, prices: {chain: price}}}
    CHAINS_ORDER = ["selver", "rimi", "prisma", "coop", "maxima"]
    pivot: dict = {}
    for r in price_rows:
        gid = r["group_id"]
        if gid not in pivot:
            pivot[gid] = {"name": r["name"], "prices": {}}
        pivot[gid]["prices"][r["chain"]] = float(r["price"])

    price_table_rows = ""
    price_mobile_cards = ""
    for gid, data in pivot.items():
        chain_prices = data["prices"]
        known_prices = [p for p in chain_prices.values() if p is not None]
        min_p = min(known_prices) if known_prices else None
        max_p = max(known_prices) if known_prices else None
        cells = ""
        card_rows = ""
        for ch in CHAINS_ORDER:
            p = chain_prices.get(ch)
            chain_label = ch.capitalize()
            if p is None:
                cells += '<td class="price-cell missing">—</td>'
                card_rows += f'<div class="price-card-row"><span class="price-card-chain">{chain_label}</span><span class="price-card-price missing">—</span></div>'
            elif min_p is not None and p == min_p and min_p != max_p:
                cells += f'<td class="price-cell best">{p:.2f} €</td>'
                card_rows += f'<div class="price-card-row"><span class="price-card-chain">{chain_label}</span><span class="price-card-price best">{p:.2f} €</span></div>'
            elif max_p is not None and p == max_p and min_p != max_p:
                cells += f'<td class="price-cell highest">{p:.2f} €</td>'
                card_rows += f'<div class="price-card-row"><span class="price-card-chain">{chain_label}</span><span class="price-card-price highest">{p:.2f} €</span></div>'
            else:
                cells += f'<td class="price-cell">{p:.2f} €</td>'
                card_rows += f'<div class="price-card-row"><span class="price-card-chain">{chain_label}</span><span class="price-card-price">{p:.2f} €</span></div>'

        if min_p is not None and max_p is not None and min_p > 0:
            spread_abs = max_p - min_p
            spread_pct = round(spread_abs / min_p * 100, 1)
            spread_pct_text = str(spread_pct).replace('.', ',')
            spread = f"{spread_abs:.2f} € · {spread_pct_text}%"
        else:
            spread = "—"

        price_table_rows += f"""
        <tr>
            <td class="price-product-name">{escape(data['name'])}</td>
            {cells}
            <td class="price-spread">{spread}</td>
        </tr>"""

        price_mobile_cards += f"""
        <div class="price-card">
            <div class="price-card-title">{escape(data['name'])}</div>
            {card_rows}
            <div class="price-card-spread">Vahe: {spread}</div>
        </div>"""

    if not price_table_rows:
        price_table_rows = '<tr><td colspan="7" style="text-align:center;color:#9198A3;padding:20px">Hinnaandmeid ei leitud.</td></tr>'
        price_mobile_cards = '<div class="empty-state">Hinnaandmeid ei leitud.</div>'

    # --- Sortimendi katvus kettides ---
    # Mitu brändi gruppi on igas ketis hinnaga esindatud, kogu grupiarvust.
    coverage_rows = await conn.fetch("""
        SELECT LOWER(s.chain) AS chain, COUNT(DISTINCT pgm.group_id) AS covered
        FROM product_group_members pgm
        JOIN prices pr ON pr.product_id = pgm.product_id
        JOIN stores s ON s.id = pr.store_id
        WHERE pgm.group_id = ANY($1::int[])
          AND pr.collected_at > NOW() - INTERVAL '14 days'
          AND pr.price > 0
        GROUP BY LOWER(s.chain)
    """, brand_group_ids)
    coverage_by_chain = {r["chain"]: r["covered"] for r in coverage_rows}
    total_groups = len(brand_group_ids)
    coverage_html = "".join(
        f"""<div class="coverage-item">
            <span class="coverage-chain">{ch.capitalize()}</span>
            <span class="coverage-count">{coverage_by_chain.get(ch, 0)}/{total_groups}{f" · {round(coverage_by_chain.get(ch, 0) / total_groups * 100)}%" if total_groups else ""}</span>
        </div>"""
        for ch in CHAINS_ORDER
    )

    # --- Müügivõimalused kettide lõikes ---
    # Brändi tooted, mille vastu on nõudlust (vaatamised+lisamised), aga
    # mis PUUDUVAD konkreetsest ketist. Top 5 toodet ketti kohta.
    opportunity_rows = await conn.fetch("""
        WITH demand AS (
            SELECT pgm.group_id, COUNT(*) AS demand_count
            FROM analytics_events a
            JOIN product_group_members pgm ON pgm.product_id = a.product_id
            WHERE pgm.group_id = ANY($1::int[])
              AND a.event_type IN ('product_view', 'basket_add')
              AND a.created_at >= CURRENT_DATE - ($2::int - 1)
              AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
            GROUP BY pgm.group_id
        ),
        group_names AS (
            SELECT id AS group_id, COALESCE(canonical_name, 'Toode #' || id) AS name
            FROM product_groups WHERE id = ANY($1::int[])
        )
        SELECT gn.group_id, gn.name, d.demand_count, missing_chain.chain
        FROM group_names gn
        JOIN demand d ON d.group_id = gn.group_id
        CROSS JOIN (VALUES ('selver'),('rimi'),('prisma'),('coop'),('maxima')) AS missing_chain(chain)
        WHERE NOT EXISTS (
            SELECT 1 FROM product_group_members pgm2
            JOIN prices pr2 ON pr2.product_id = pgm2.product_id
            JOIN stores st2 ON st2.id = pr2.store_id
            WHERE pgm2.group_id = gn.group_id
              AND LOWER(st2.chain) = missing_chain.chain
              AND pr2.collected_at > NOW() - INTERVAL '14 days'
              AND pr2.price > 0
        )
        ORDER BY missing_chain.chain, d.demand_count DESC
    """, brand_group_ids, days)

    opportunities_by_chain: dict = {}
    for r in opportunity_rows:
        opportunities_by_chain.setdefault(r["chain"], []).append(r)

    opportunity_sections_html = ""
    for ch in CHAINS_ORDER:
        items = opportunities_by_chain.get(ch, [])[:5]
        if not items:
            continue
        items_html = "".join(
            f'<li><span class="opportunity-name">{escape(r["name"])}</span><span class="opportunity-demand">{r["demand_count"]} nõudlust</span></li>'
            for r in items
        )
        opportunity_sections_html += f"""
        <div class="opportunity-chain-block">
            <div class="opportunity-chain-title">{ch.capitalize()}</div>
            <ul class="opportunity-list">{items_html}</ul>
        </div>"""

    if not opportunity_sections_html:
        opportunity_sections_html = '<div class="empty-state">Praegu ei tuvastatud müügivõimalusi — teie tooted on hästi esindatud kõigis kettides.</div>'

    # --- Kiiremini kasvavad tooted (võrreldes eelmise sama pika perioodiga) ---
    momentum_rows = await conn.fetch("""
        WITH current_period AS (
            SELECT product_id, COUNT(*) AS cnt
            FROM analytics_events
            WHERE product_id = ANY($1::int[])
              AND event_type = 'basket_add'
              AND created_at >= CURRENT_DATE - ($2::int - 1)
              AND created_at < CURRENT_DATE + INTERVAL '1 day'
            GROUP BY product_id
        ),
        previous_period AS (
            SELECT product_id, COUNT(*) AS cnt
            FROM analytics_events
            WHERE product_id = ANY($1::int[])
              AND event_type = 'basket_add'
              AND created_at >= CURRENT_DATE - (($2::int * 2) - 1)
              AND created_at < CURRENT_DATE - ($2::int - 1)
            GROUP BY product_id
        )
        SELECT p.id, p.name,
               COALESCE(c.cnt, 0) AS current_cnt,
               COALESCE(pr.cnt, 0) AS prev_cnt
        FROM products p
        LEFT JOIN current_period c ON c.product_id = p.id
        LEFT JOIN previous_period pr ON pr.product_id = p.id
        WHERE p.id = ANY($1::int[])
          AND (COALESCE(c.cnt, 0) > 0 OR COALESCE(pr.cnt, 0) > 0)
    """, brand_product_ids, days)

    MIN_CURRENT_FOR_MOMENTUM = 3
    MIN_PREVIOUS_FOR_PERCENT = 2
    momentum_list = []
    for r in momentum_rows:
        cur = r["current_cnt"] or 0
        prev = r["prev_cnt"] or 0
        if cur < MIN_CURRENT_FOR_MOMENTUM:
            continue
        if prev == 0:
            momentum_list.append((r["name"], cur, None))  # None = "Uus"
        elif prev >= MIN_PREVIOUS_FOR_PERCENT:
            growth = round((cur - prev) / prev * 100, 1)
            if growth > 0:
                momentum_list.append((r["name"], cur, growth))
    momentum_list = sorted(
        momentum_list,
        key=lambda x: (
            x[1],                              # praegune maht
            x[2] if x[2] is not None else 999  # kasv/uudsus
        ),
        reverse=True,
    )[:5]

    momentum_html = "".join(
        f"""<div class="product-row">
            <span class="product-rank">{i}</span>
            <div class="product-content">
                <div class="product-name">{escape(name)}</div>
            </div>
            <div class="product-count" style="color:{'#1B9A59' if growth is not None else 'var(--accent-dark)'}">
                {f"↑ {str(growth).replace('.', ',')}% · {cur} lisamist" if growth is not None else f"Uus · {cur} lisamist"}
            </div>
        </div>"""
        for i, (name, cur, growth) in enumerate(momentum_list, 1)
    ) or '<div class="empty-state">Valitud perioodil ei tuvastatud kasvavaid tooteid.</div>'

    # --- Huvi vs ostusoov ---
    # Tooted, mille vastu on suur vaatamishuvi, aga madal korvi lisamise
    # määr — signaal võimalikest probleemidest (hind, pakend, positsioon
    # võrdluses). Nõuab vähemalt 5 vaatamist, et vältida juhuslikku müra
    # väikese valimi korral.
    interest_rows = await conn.fetch("""
        SELECT a.product_id, p.name,
            COUNT(*) FILTER (WHERE a.event_type = 'product_view') AS views,
            COUNT(*) FILTER (WHERE a.event_type = 'basket_add') AS adds
        FROM analytics_events a
        JOIN products p ON p.id = a.product_id
        WHERE a.product_id = ANY($1::int[])
          AND a.created_at >= CURRENT_DATE - ($2::int - 1)
          AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
        GROUP BY a.product_id, p.name
        HAVING COUNT(*) FILTER (WHERE a.event_type = 'product_view') >= 5
        ORDER BY (
            COUNT(*) FILTER (WHERE a.event_type = 'basket_add')::float
            / NULLIF(COUNT(*) FILTER (WHERE a.event_type = 'product_view'), 0)
        ) ASC
        LIMIT 8
    """, brand_product_ids, days)

    interest_html = "".join(
        f"""<div class="interest-row">
            <div class="interest-name" title="{escape(r['name'] or '')}">{escape(r['name'] or '—')}</div>
            <div class="interest-stats">
                <span>{r['views']:,} vaatamist</span>
                <span>·</span>
                <span>{r['adds']:,} lisamist</span>
                <span>·</span>
                <span class="interest-rate">{round(r['adds']/r['views']*100, 1)}% määr</span>
            </div>
        </div>"""
        for r in interest_rows
    ) or '<div class="empty-state">Valitud perioodil pole piisavalt vaatamisi analüüsiks.</div>'

    # --- Hinnapositsiooni kokkuvõte ---
    # Kokkuvõte juba arvutatud pivot-andmetest — kiire ülevaade ilma
    # tabelit lugemata.
    spread_values = []
    for gid, data in pivot.items():
        prices_known = [p for p in data["prices"].values() if p is not None]
        if len(prices_known) >= 2:
            spread_values.append(max(prices_known) - min(prices_known))
    price_summary_tracked = len(pivot)
    price_summary_avg_spread = (sum(spread_values) / len(spread_values)) if spread_values else None
    price_summary_max_spread = max(spread_values) if spread_values else None

    def delta_html(current, previous):
        if previous == 0:
            if current > 0:
                return '<span style="color:#1B9A59;font-size:12px;font-weight:600">Uus aktiivsus</span>'
            return ''
        pct = round((current - previous) / previous * 100, 1)
        pct_text = str(abs(pct)).replace('.', ',')
        if pct > 0:
            return f'<span style="color:#1B9A59;font-size:12px;font-weight:600">↑ {pct_text}%</span>'
        elif pct < 0:
            return f'<span style="color:#e74c3c;font-size:12px;font-weight:600">↓ {pct_text}%</span>'
        return '<span style="color:#9198A3;font-size:12px">Muutuseta</span>'

    total_adds = totals["total_adds"] or 0
    total_views = totals["total_views"] or 0
    unique_visitors = totals["unique_visitors"] or 0
    prev_unique_visitors = prev_totals["unique_visitors"] or 0
    basket_add_rate = round(total_adds / total_views * 100, 1) if total_views > 0 else None

    # Brand view has no admin exemption concept (there is no "admin
    # viewing a brand" case) — privacy threshold always applies here.
    PRIVACY_THRESHOLD = 10
    if unique_visitors < PRIVACY_THRESHOLD:
        visitors_display = "&lt; 10"
        visitors_note_html = (
            'Privaatsuslävi rakendatud'
            '<span class="info-dot" title="Loendab sisseloginud kasutajaid ja pseudonüümseid seadmetunnuseid. '
            'Sama inimene mitmes seadmes võib lugeda mitme külastajana.">i</span>'
        )
    else:
        visitors_display = f"{unique_visitors:,}"
        visitors_note_html = f'Aktiivsed külastajad {delta_html(unique_visitors, prev_unique_visitors)}'

    max_adds = max((r["adds"] for r in top_products_rows), default=0)
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

    daily_dict: dict = {}
    for r in daily_rows:
        d = str(r["day"])
        daily_dict.setdefault(d, {})[r["event_type"]] = r["cnt"]

    today = datetime.datetime.now(datetime.timezone.utc).date()
    all_days = [today - datetime.timedelta(days=offset) for offset in range(days - 1, -1, -1)]
    daily_labels_json = json.dumps([d.strftime("%m-%d") for d in all_days], ensure_ascii=False)
    daily_adds_json = json.dumps([daily_dict.get(str(d), {}).get("basket_add", 0) for d in all_days])
    daily_views_json = json.dumps([daily_dict.get(str(d), {}).get("product_view", 0) for d in all_days])
    chart_height = 250 if days <= 7 else 355

    rates_html = ""
    if basket_add_rate is not None:
        rates_html = f'<div style="font-size:12px;color:var(--text-secondary);margin-top:6px">Korvi lisamise määr: <b style="color:var(--text)">{basket_add_rate}%</b> vaatamistest</div>'

    days_btns = "".join(
        f'<a class="filter-pill{"  active" if period == days else ""}" href="/admin/analytics?days={period}">{period}p</a>'
        for period in [7, 14, 30, 90]
    )

    title = f"{escape(partner_name)} — viimased {days} päeva"

    html = f"""<!DOCTYPE html>
<html lang="et">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="color-scheme" content="light">
<title>Seivy Analytics — {escape(partner_name)}</title>
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
.brand-name {{ margin: 0; font-size: 18px; font-weight: 750; letter-spacing: -.03em; display: flex; align-items: center; gap: 8px; }}
.partner-badge {{ display: inline-flex; align-items: center; gap: 6px; padding: 3px 9px; border-radius: 999px; background: var(--accent-soft); color: var(--accent-dark); font-size: 11px; font-weight: 750; letter-spacing: .02em; }}
.partner-badge-dot {{ width: 6px; height: 6px; border-radius: 999px; background: var(--accent); }}
.brand-section {{ margin: 2px 0 0; color: var(--text-secondary); font-size: 12px; font-weight: 550; }}
.topbar-context {{ display: flex; align-items: center; gap: 10px; color: var(--text-secondary); font-size: 13px; font-weight: 600; }}
.live-dot {{ width: 8px; height: 8px; border-radius: 999px; background: var(--success); box-shadow: 0 0 0 4px var(--success-soft); }}
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
.metrics-grid {{ display: grid; grid-template-columns: repeat(4,minmax(0,1fr)); margin-bottom: 22px; gap: 16px; }}
.metric-card {{ position: relative; overflow: hidden; padding: 22px; border: 1px solid var(--border); border-radius: var(--radius-lg); background: var(--surface); box-shadow: var(--shadow); }}
.metric-card::after {{ position: absolute; top: -48px; right: -48px; width: 116px; height: 116px; border-radius: 999px; background: var(--accent-soft); content: ""; opacity: .65; }}
.metric-top {{ position: relative; z-index: 1; display: flex; align-items: center; justify-content: space-between; margin-bottom: 19px; gap: 16px; }}
.metric-label {{ color: var(--text-secondary); font-size: 13px; font-weight: 650; }}
.metric-icon {{ display: grid; width: 38px; height: 38px; place-items: center; border-radius: 11px; background: var(--accent-soft); color: var(--accent-dark); }}
.metric-icon svg {{ width: 19px; height: 19px; stroke: currentColor; }}
.metric-value {{ position: relative; z-index: 1; margin: 0; font-size: clamp(27px,2.4vw,38px); font-weight: 770; letter-spacing: -.045em; line-height: 1; }}
.metric-note {{ position: relative; z-index: 1; margin: 10px 0 0; color: var(--text-muted); font-size: 12px; font-weight: 550; display: flex; align-items: center; flex-wrap: wrap; gap: 4px; }}
.info-dot {{ display: inline-grid; place-items: center; width: 16px; height: 16px; margin-left: 5px; border-radius: 999px; background: var(--surface-muted); color: var(--text-muted); font-size: 10px; font-weight: 800; cursor: help; }}
.dashboard-grid {{ display: grid; grid-template-columns: minmax(0,1.6fr) minmax(330px,.8fr); gap: 22px; }}
.dashboard-column {{ display: flex; flex-direction: column; gap: 22px; }}
.panel {{ padding: 24px; border: 1px solid var(--border); border-radius: var(--radius-lg); background: var(--surface); box-shadow: var(--shadow); }}
.panel-full {{ grid-column: 1 / -1; }}
.panel-header {{ display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 23px; gap: 18px; }}
.panel-title {{ margin: 0; color: var(--text); font-size: 17px; font-weight: 730; letter-spacing: -.025em; }}
.panel-description {{ margin: 6px 0 0; color: var(--text-secondary); font-size: 12px; line-height: 1.5; }}
.panel-badge {{ padding: 7px 10px; border-radius: 999px; background: var(--surface-muted); color: var(--text-secondary); font-size: 11px; font-weight: 700; }}
.chart-wrapper {{ position: relative; width: 100%; height: {chart_height}px; }}
.product-list {{ display: flex; flex-direction: column; gap: 2px; }}
.product-row {{ display: grid; grid-template-columns: 32px minmax(0,1fr) auto; align-items: center; padding: 13px 4px; border-bottom: 1px solid var(--border); gap: 12px; }}
.product-row:last-child {{ border-bottom: 0; }}
.product-rank {{ display: grid; width: 28px; height: 28px; place-items: center; border-radius: 8px; background: var(--surface-muted); color: var(--text-secondary); font-size: 11px; font-weight: 750; }}
.product-row:first-child .product-rank {{ background: var(--accent-soft); color: var(--accent-dark); }}
.product-name {{ margin-bottom: 4px; overflow: hidden; color: var(--text); font-size: 13px; font-weight: 650; line-height: 1.35; text-overflow: ellipsis; white-space: nowrap; }}
.product-bar-track {{ width: 100%; height: 5px; overflow: hidden; border-radius: 999px; background: #ECEFF3; }}
.product-bar-fill {{ display: block; width: var(--bar-width,0%); height: 100%; border-radius: inherit; background: var(--accent); }}
.product-count {{ min-width: 58px; color: var(--text); font-size: 13px; font-weight: 750; text-align: right; }}
.product-count small {{ display: block; margin-top: 2px; color: var(--text-muted); font-size: 9px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }}
.empty-state {{ display: grid; min-height: 180px; place-items: center; padding: 24px; border: 1px dashed var(--border-strong); border-radius: var(--radius-md); background: var(--surface-muted); color: var(--text-secondary); font-size: 13px; text-align: center; }}
.footer {{ display: flex; align-items: center; justify-content: space-between; margin-top: 24px; padding: 0 4px; gap: 20px; color: var(--text-muted); font-size: 11px; }}
.price-table-wrapper {{ overflow-x: auto; }}
.price-table {{ width: 100%; border-collapse: collapse; min-width: 640px; }}
.price-table th {{ text-align: left; padding: 10px 12px; font-size: 11px; font-weight: 700; color: var(--text-secondary); text-transform: uppercase; letter-spacing: .04em; border-bottom: 1px solid var(--border); }}
.price-table td {{ padding: 10px 12px; font-size: 13px; border-bottom: 1px solid var(--border); }}
.price-product-name {{ font-weight: 650; color: var(--text); max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.price-cell {{ text-align: right; font-weight: 650; }}
.price-cell.best {{ background: #EAF8F0; color: #127A45; border-radius: 6px; }}
.price-cell.highest {{ background: #FFF4E5; color: #A85600; border-radius: 6px; }}
.price-cell.missing {{ background: #F1F3F6; color: var(--text-muted); text-align: center; border-radius: 6px; }}
.price-spread {{ text-align: right; color: var(--text-secondary); font-weight: 650; }}
.coverage-grid {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; }}
.coverage-item {{ display: flex; flex-direction: column; align-items: center; gap: 6px; padding: 14px 8px; border: 1px solid var(--border); border-radius: var(--radius-md); background: var(--surface-muted); }}
.coverage-chain {{ font-size: 12px; font-weight: 700; color: var(--text-secondary); }}
.coverage-count {{ font-size: 18px; font-weight: 800; color: var(--text); }}
.opportunity-chain-block {{ margin-bottom: 16px; }}
.opportunity-chain-block:last-child {{ margin-bottom: 0; }}
.opportunity-chain-title {{ font-size: 12px; font-weight: 750; color: var(--accent-dark); text-transform: uppercase; letter-spacing: .04em; margin-bottom: 8px; }}
.opportunity-list {{ list-style: none; margin: 0; padding: 0; }}
.opportunity-list li {{ display: flex; align-items: center; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--border); font-size: 13px; }}
.opportunity-list li:last-child {{ border-bottom: 0; }}
.opportunity-name {{ color: var(--text); font-weight: 650; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 70%; }}
.opportunity-demand {{ color: var(--text-muted); font-size: 12px; font-weight: 650; white-space: nowrap; }}
.interest-row {{ padding: 12px 4px; border-bottom: 1px solid var(--border); }}
.interest-row:last-child {{ border-bottom: 0; }}
.interest-name {{ font-size: 13px; font-weight: 650; color: var(--text); margin-bottom: 5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.interest-stats {{ display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-secondary); }}
.interest-rate {{ font-weight: 750; color: var(--accent-dark); }}
.price-summary-badges {{ display: flex; gap: 8px; flex-wrap: wrap; }}
.price-summary-badge {{ padding: 6px 12px; border-radius: 999px; background: var(--surface-muted); color: var(--text-secondary); font-size: 12px; font-weight: 650; }}
.price-summary-badge strong {{ color: var(--text); }}
@media (max-width: 760px) {{
    .coverage-grid {{ grid-template-columns: repeat(2, 1fr); }}
}}
.mobile-price-cards {{ display: none; }}
.export-btn {{ display: inline-flex; align-items: center; gap: 6px; padding: 8px 14px; border: 1px solid var(--border); border-radius: 999px; background: var(--surface); color: var(--text-secondary); font-size: 12px; font-weight: 650; text-decoration: none; transition: border-color 160ms,background 160ms; }}
.export-btn-disabled {{ opacity: .55; cursor: not-allowed; background: var(--surface-muted); pointer-events: none; }}
@media (max-width: 760px) {{
    .desktop-price-table {{ display: none; }}
    .mobile-price-cards {{ display: flex; flex-direction: column; gap: 12px; }}
    .price-card {{ border: 1px solid var(--border); border-radius: var(--radius-md); background: var(--surface-muted); padding: 14px; }}
    .price-card-title {{ font-size: 13px; font-weight: 750; color: var(--text); margin-bottom: 10px; line-height: 1.35; }}
    .price-card-row {{ display: flex; align-items: center; justify-content: space-between; padding: 8px 0; border-top: 1px solid var(--border); font-size: 13px; }}
    .price-card-chain {{ color: var(--text-secondary); font-weight: 650; }}
    .price-card-price {{ font-weight: 750; padding: 4px 8px; border-radius: 8px; }}
    .price-card-price.best {{ background: #EAF8F0; color: #127A45; }}
    .price-card-price.highest {{ background: #FFF4E5; color: #A85600; }}
    .price-card-price.missing {{ background: #F1F3F6; color: var(--text-muted); }}
    .price-card-spread {{ margin-top: 10px; padding-top: 10px; border-top: 1px solid var(--border); color: var(--text-secondary); font-size: 12px; font-weight: 650; }}
}}
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
        <p class="brand-name">Seivy <span class="partner-badge"><span class="partner-badge-dot"></span>Tootja</span></p>
        <p class="brand-section">Partneranalüütika</p>
      </div>
    </div>
    <div class="topbar-context">
      <span class="live-dot"></span>
      Viimane sündmus: {last_event_str}
      &nbsp;·&nbsp;<a href="/admin/analytics/logout" style="color:var(--text-secondary);font-size:12px;text-decoration:none">Logi välja</a>
    </div>
  </div>
</header>

<main class="content">
  <section class="page-heading">
    <div>
      <div class="eyebrow">Tootja ülevaade</div>
      <h1>Teie toodete tulemuslikkus</h1>
      <p style="margin:6px 0 0;color:var(--text-secondary);font-size:16px;font-weight:500">{title}</p>
      <p class="heading-description">Ülevaade sellest, kuidas kasutajad {escape(partner_name)} tooteid vaatavad, ostukorvi lisavad, ning kuidas teie toodete hinnad erinevad kettide vahel.</p>
      {rates_html}
    </div>
    <div style="display:flex;flex-direction:column;align-items:flex-end;gap:10px">
      <div class="period-summary"><span>Analüüsiperiood</span><strong>Viimased {days} päeva</strong></div>
      <a class="export-btn" href="/admin/analytics/export?days={days}">⬇ Ekspordi CSV</a>
    </div>
  </section>

  <section class="filters-panel">
    <div class="filter-groups">
      <div class="filter-group">
        <div class="filter-label">Periood</div>
        <nav class="filter-pills">{days_btns}</nav>
      </div>
    </div>
  </section>

  <section class="metrics-grid">
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Korvi lisamised</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 4h2l2.3 10.2a2 2 0 0 0 2 1.6h7.7a2 2 0 0 0 1.9-1.4L21 8H7"></path><path d="M12 6v6"></path><path d="M9 9h6"></path><circle cx="10" cy="20" r="1"></circle><circle cx="18" cy="20" r="1"></circle></svg></span>
      </div>
      <p class="metric-value">{total_adds:,}</p>
      <p class="metric-note">Teie toodete lisamised korvidesse</p>
    </article>
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Toote vaatamised</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z"></path><circle cx="12" cy="12" r="2.5"></circle></svg></span>
      </div>
      <p class="metric-value">{total_views:,}</p>
      <p class="metric-note">Teie tootekaartide avamised</p>
    </article>
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Korvi lisamise määr</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"></path><path d="M7 14l4-4 3 3 5-6"></path></svg></span>
      </div>
      <p class="metric-value">{f"{basket_add_rate}%" if basket_add_rate is not None else "—"}</p>
      <p class="metric-note">Vaatamistest korvi lisamiseni</p>
    </article>
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Unikaalsed külastajad</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="8" r="3"></circle><path d="M3 20a6 6 0 0 1 12 0"></path><circle cx="17" cy="9" r="2"></circle><path d="M15.5 15.5A5 5 0 0 1 21 20"></path></svg></span>
      </div>
      <p class="metric-value">{visitors_display}</p>
      <p class="metric-note">{visitors_note_html}</p>
    </article>
  </section>

  <section class="panel" style="margin-bottom:22px">
    <div class="panel-header">
      <div>
        <h2 class="panel-title">Hinnapositsioon kettide vahel</h2>
        <p class="panel-description">Teie enim nõudlust saanud toodete hetkehinnad viies suuremas ketis. Roheline = odavaim leitud hind, oranž = kõrgeim leitud hind. Vahe = kõrgeima ja madalaima leitud hinna erinevus.</p>
        <div class="price-summary-badges" style="margin-top:12px">
          <span class="price-summary-badge">Jälgitud tooteid: <strong>{price_summary_tracked}</strong></span>
          <span class="price-summary-badge">Keskmine hinnavahe: <strong>{f"{price_summary_avg_spread:.2f} €" if price_summary_avg_spread is not None else "—"}</strong></span>
          <span class="price-summary-badge">Suurim hinnavahe: <strong>{f"{price_summary_max_spread:.2f} €" if price_summary_max_spread is not None else "—"}</strong></span>
        </div>
      </div>
    </div>
    <div class="price-table-wrapper desktop-price-table">
      <table class="price-table">
        <tr>
          <th>Toode</th>
          <th style="text-align:right">Selver</th>
          <th style="text-align:right">Rimi</th>
          <th style="text-align:right">Prisma</th>
          <th style="text-align:right">Coop</th>
          <th style="text-align:right">Maxima</th>
          <th style="text-align:right">Vahe</th>
        </tr>
        {price_table_rows}
      </table>
    </div>
    <div class="mobile-price-cards">
      {price_mobile_cards}
    </div>
  </section>

  <section class="panel" style="margin-bottom:22px">
    <div class="panel-header">
      <div>
        <h2 class="panel-title">Sortimendi katvus kettides</h2>
        <p class="panel-description">Mitu teie toodet ({total_groups} kokku) on igas ketis praegu saadaval.</p>
      </div>
    </div>
    <div class="coverage-grid">{coverage_html}</div>
  </section>

  <section class="panel" style="margin-bottom:22px">
    <div class="panel-header">
      <div>
        <h2 class="panel-title">Müügivõimalused kettide lõikes</h2>
        <p class="panel-description">Tooted, mille vastu oli Seivy kasutajatel nõudlust, kuid mille aktiivset hinda või sortimendi vastet konkreetses ketis Seivy andmetes ei leitud. Sobib sisendiks sortimendi- ja müügivestlustele.</p>
      </div>
    </div>
    {opportunity_sections_html}
  </section>

  <section class="dashboard-grid">
    <div class="dashboard-column">
      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Igapäevane aktiivsus</h2>
            <p class="panel-description">Toote vaatamiste ja korvi lisamiste muutus päevade lõikes.</p>
          </div>
          <span class="panel-badge">{days} päeva</span>
        </div>
        <div class="chart-wrapper">
          <canvas id="brandDailyChart"></canvas>
        </div>
      </article>
    </div>

    <div class="dashboard-column">
      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Populaarseimad tooted brändi seest</h2>
            <p class="panel-description">Teie tooted, mis on kasutajate seas kõige rohkem korvi lisatud.</p>
          </div>
        </div>
        <div class="product-list">{products_html}</div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Kiiremini kasvavad tooted</h2>
            <p class="panel-description">Võrreldes eelmise sama pika perioodiga.</p>
          </div>
        </div>
        <div class="product-list">{momentum_html}</div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Huvi vs ostusoov</h2>
            <p class="panel-description">Tooted, mida palju vaadatakse, aga harva korvi lisatakse — võimalik hinna, pakendi või positsioneerimise signaal.</p>
          </div>
        </div>
        {interest_html}
      </article>
    </div>
  </section>


  <footer class="footer">
    <span>Näitajad põhinevad Seivy rakenduses kogutud koondatud kasutussündmustel. Unikaalsed külastajad arvestavad sisselogitud kasutajaid ja pseudonüümseid seadmetunnuseid.</span>
    <span>Seivy partneranalüütika</span>
  </footer>
</main>

<script>
(function() {{
  "use strict";
  const labels = {daily_labels_json};
  const dailyAdds = {daily_adds_json};
  const dailyViews = {daily_views_json};

  Chart.defaults.font.family = 'Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif';
  Chart.defaults.color = "#68707D";
  Chart.defaults.borderColor = "#E9ECF1";

  const sharedScales = {{
    x: {{ grid: {{ display: false }}, border: {{ display: false }}, ticks: {{ color: "#7A828E", font: {{ size: 11, weight: "600" }}, maxRotation: 0, autoSkip: true, maxTicksLimit: 8 }} }},
    y: {{ beginAtZero: true, grid: {{ color: "#EEF0F4", drawTicks: false }}, border: {{ display: false }}, ticks: {{ color: "#8B929D", padding: 10, precision: 0, font: {{ size: 10, weight: "600" }} }} }}
  }};

  const barCanvas = document.getElementById("brandDailyChart");
  if (barCanvas) {{
    new Chart(barCanvas, {{
      type: "bar",
      data: {{
        labels,
        datasets: [
          {{ label: "Korvi lisamised", data: dailyAdds, backgroundColor: "#FF9100", borderRadius: 6, maxBarThickness: 26 }},
          {{ label: "Toote vaatamised", data: dailyViews, backgroundColor: "#FFD7A3", borderRadius: 6, maxBarThickness: 26 }}
        ]
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        interaction: {{ mode: "index", intersect: false }},
        plugins: {{
          legend: {{ position: "top", align: "end", labels: {{ usePointStyle: true, pointStyle: "rectRounded", boxWidth: 8, boxHeight: 8, padding: 18, font: {{ size: 11, weight: "650" }} }} }}
        }},
        scales: sharedScales
      }}
    }});
  }}
}})();
</script>
</body></html>"""
    resp = HTMLResponse(html)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers["Cache-Control"] = "private, no-store"
    return resp


@router.get("/admin/analytics", response_class=HTMLResponse)
async def analytics_dashboard(request: Request, token: str = None, days: int = 30, chain: str = None):
    import json, os
    from html import escape
    from fastapi.responses import RedirectResponse

    TOKEN_MAP = {
        os.environ.get("ANALYTICS_TOKEN_SELVER", ""): "selver",
        os.environ.get("ANALYTICS_TOKEN_RIMI", ""): "rimi",
        os.environ.get("ANALYTICS_TOKEN_PRISMA", ""): "prisma",
        os.environ.get("ANALYTICS_TOKEN_COOP", ""): "coop",
        os.environ.get("ANALYTICS_TOKEN_MAXIMA", ""): "maxima",
    }
    TOKEN_MAP.pop("", None)

    admin_token = os.environ.get("ANALYTICS_TOKEN_ADMIN", "")

    COOKIE_NAME = "seivy_analytics_token"
    cookie_token = request.cookies.get(COOKIE_NAME)
    resolved_token = token or cookie_token

    if not resolved_token:
        return HTMLResponse("<h2>Ligipääs keelatud. Token puudub.</h2>", status_code=403)

    ALLOWED_DAYS = {7, 14, 30, 90}
    if days not in ALLOWED_DAYS:
        days = 30

    is_admin = admin_token and resolved_token == admin_token
    partner_type = None
    partner_name = None
    partner_brand_filter = None
    if is_admin:
        locked_chain = None
        partner_type = "admin"
    elif resolved_token in TOKEN_MAP:
        locked_chain = TOKEN_MAP[resolved_token]
        partner_type = "retailer"
    else:
        locked_chain = None

    allowed_chains = {"selver", "rimi", "prisma", "coop", "maxima"}
    if chain:
        chain = chain.lower().strip()
        if chain not in allowed_chains:
            chain = None

    # DB must be ready before we can look up analytics_partners tokens —
    # moved this check ahead of the redirect block (previously it ran
    # after the redirect, which meant a brand/partner token could be
    # written into a cookie before ever being validated against the
    # database).
    if getattr(request.app.state, "db", None) is None:
        return HTMLResponse("<h2>DB not ready yet.</h2>", status_code=503)

    if partner_type is None:
        # Not admin, not a legacy chain env-var token — check the
        # analytics_partners table (brand partners, and any newer
        # retailer partners added via the /admin/partners UI instead of
        # env vars). Resolved here, BEFORE the redirect below, so an
        # invalid token never gets written into a cookie, and a valid
        # brand token's "no chain" state is correctly reflected in the
        # redirect URL.
        async with request.app.state.db.acquire() as conn:
            partner_row = await conn.fetchrow("""
                SELECT partner_type, name, brand_filter, chain_filter
                FROM analytics_partners
                WHERE token = $1
            """, resolved_token)

        if not partner_row:
            return HTMLResponse("<h2>Ligipääs keelatud. Token on vale.</h2>", status_code=403)

        partner_type = partner_row["partner_type"]
        partner_name = partner_row["name"]

        if partner_type == "brand":
            partner_brand_filter = partner_row["brand_filter"] or []
            if not partner_brand_filter:
                return HTMLResponse("<h2>Partnerile pole brändi määratud. Võta ühendust administraatoriga.</h2>", status_code=500)
        elif partner_type == "retailer":
            locked_chain = (partner_row["chain_filter"] or "").lower().strip() or None
            if not locked_chain:
                return HTMLResponse("<h2>Partnerile pole ketti määratud. Võta ühendust administraatoriga.</h2>", status_code=500)
        else:
            return HTMLResponse("<h2>Ligipääs keelatud. Tundmatu partneri tüüp.</h2>", status_code=403)

    if not is_admin and locked_chain:
        chain = locked_chain

    # Redirect to strip the token out of the URL and store it in a
    # cookie instead — now happens AFTER full token validation above, so
    # we never set a cookie for a token that turned out to be invalid,
    # and the chain_part below correctly reflects "no chain" for brand
    # partners (chain stays None for them) vs the locked chain for
    # retailer/admin.
    if token:
        chain_part = f"&chain={chain}" if chain else ""
        redirect_url = f"/admin/analytics?days={days}{chain_part}"
        response = RedirectResponse(url=redirect_url, status_code=302)
        response.set_cookie(
            key=COOKIE_NAME,
            value=resolved_token,
            httponly=True,
            secure=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 30,
            path="/admin/analytics"
        )
        return response

    if partner_type == "brand":
        async with request.app.state.db.acquire() as conn:
            return await _render_brand_dashboard(
                conn=conn,
                partner_name=partner_name,
                brand_filter=partner_brand_filter,
                days=days,
            )

    async with request.app.state.db.acquire() as conn:
        all_wins_total = await conn.fetchval("""
            SELECT COUNT(*) FROM analytics_events
            WHERE event_type = 'basket_win'
              AND LOWER(chain) IN ('selver', 'rimi', 'prisma', 'coop', 'maxima')
              AND created_at >= CURRENT_DATE - ($1::int - 1)
              AND created_at < CURRENT_DATE + INTERVAL '1 day'
        """, days)

        basket_wins_rows = await conn.fetch("""
            WITH chains(chain) AS (
                VALUES ('selver'), ('rimi'), ('prisma'), ('coop'), ('maxima')
            ),
            wins AS (
                SELECT LOWER(chain) AS chain, COUNT(*) AS wins
                FROM analytics_events
                WHERE event_type = 'basket_win'
                  AND created_at >= CURRENT_DATE - ($1::int - 1)
                  AND created_at < CURRENT_DATE + INTERVAL '1 day'
                GROUP BY LOWER(chain)
            ),
            results AS (
                SELECT c.chain, COALESCE(w.wins, 0) AS wins
                FROM chains c
                LEFT JOIN wins w ON w.chain = c.chain
            )
            SELECT chain, wins, RANK() OVER (ORDER BY wins DESC) AS position
            FROM results
            ORDER BY wins DESC, chain ASC
        """, days)

        top_products_rows = await conn.fetch("""
            SELECT a.product_id, p.name, a.chain, COUNT(*) AS adds
            FROM analytics_events a
            LEFT JOIN products p ON p.id = a.product_id
            WHERE a.event_type = 'basket_add'
              AND a.product_id IS NOT NULL
              AND a.created_at >= CURRENT_DATE - ($1::int - 1)
              AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
              AND ($2::text IS NULL OR LOWER(a.chain) = LOWER($2))
            GROUP BY a.product_id, p.name, a.chain
            ORDER BY adds DESC
            LIMIT 10
        """, days, chain)

        # Kaitstud try/except: kui categories_sub/categories_main skeem
        # peaks erinema oodatust, ei kuku kogu dashboard 500-ga kokku —
        # sektsioon jääb lihtsalt tühjaks ("pole andmeid").
        try:
            category_rows = await conn.fetch("""
                SELECT COALESCE(cm.label_et, 'Muu') AS category, COUNT(*) AS cnt
                FROM analytics_events a
                JOIN products p ON p.id = a.product_id
                LEFT JOIN categories_sub cs ON cs.code = p.sub_code
                LEFT JOIN categories_main cm ON cm.id = cs.main_id
                WHERE a.event_type = 'basket_add'
                  AND a.product_id IS NOT NULL
                  AND a.created_at >= CURRENT_DATE - ($1::int - 1)
                  AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
                  AND ($2::text IS NULL OR LOWER(a.chain) = LOWER($2))
                GROUP BY COALESCE(cm.label_et, 'Muu')
                ORDER BY cnt DESC
                LIMIT 8
            """, days, chain)
        except Exception as e:
            print(f"[analytics_dashboard] category_rows query failed: {e}")
            category_rows = []

        # Kiiremini kasvavad kategooriad — võrdlus eelmise sama pika
        # perioodiga. Sama try/except kaitse, kuna kasutab sama
        # categories_sub/categories_main skeemi.
        try:
            category_momentum_rows = await conn.fetch("""
                WITH current_period AS (
                    SELECT COALESCE(cm.label_et, 'Muu') AS category, COUNT(*) AS cnt
                    FROM analytics_events a
                    JOIN products p ON p.id = a.product_id
                    LEFT JOIN categories_sub cs ON cs.code = p.sub_code
                    LEFT JOIN categories_main cm ON cm.id = cs.main_id
                    WHERE a.event_type = 'basket_add'
                      AND a.created_at >= CURRENT_DATE - ($1::int - 1)
                      AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
                      AND ($2::text IS NULL OR LOWER(a.chain) = LOWER($2))
                    GROUP BY COALESCE(cm.label_et, 'Muu')
                ),
                previous_period AS (
                    SELECT COALESCE(cm.label_et, 'Muu') AS category, COUNT(*) AS cnt
                    FROM analytics_events a
                    JOIN products p ON p.id = a.product_id
                    LEFT JOIN categories_sub cs ON cs.code = p.sub_code
                    LEFT JOIN categories_main cm ON cm.id = cs.main_id
                    WHERE a.event_type = 'basket_add'
                      AND a.created_at >= CURRENT_DATE - (($1::int * 2) - 1)
                      AND a.created_at < CURRENT_DATE - ($1::int - 1)
                      AND ($2::text IS NULL OR LOWER(a.chain) = LOWER($2))
                    GROUP BY COALESCE(cm.label_et, 'Muu')
                )
                SELECT
                    COALESCE(c.category, p2.category) AS category,
                    COALESCE(c.cnt, 0) AS current_cnt,
                    COALESCE(p2.cnt, 0) AS prev_cnt
                FROM current_period c
                FULL OUTER JOIN previous_period p2 ON p2.category = c.category
                WHERE COALESCE(c.cnt, 0) > 0 OR COALESCE(p2.cnt, 0) > 0
            """, days, chain)
        except Exception as e:
            print(f"[analytics_dashboard] category_momentum_rows query failed: {e}")
            category_momentum_rows = []

        missing_rows = []
        missing_total_count = 0
        if chain:
            try:
                missing_rows_all = await conn.fetch("""
                    WITH demand AS (
                        SELECT a.product_id, COUNT(*) AS demand_count
                        FROM analytics_events a
                        WHERE a.event_type IN ('basket_add', 'product_view')
                          AND a.product_id IS NOT NULL
                          AND a.created_at >= CURRENT_DATE - ($1::int - 1)
                          AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
                        GROUP BY a.product_id
                    ),
                    demand_products AS (
                        SELECT d.product_id, d.demand_count, p.name,
                               COALESCE(pgm.group_id, -d.product_id) AS grp
                        FROM demand d
                        JOIN products p ON p.id = d.product_id
                        LEFT JOIN product_group_members pgm ON pgm.product_id = d.product_id
                    ),
                    grouped AS (
                        SELECT grp,
                               SUM(demand_count) AS demand_count,
                               (array_agg(name ORDER BY demand_count DESC))[1] AS name,
                               array_agg(product_id) AS product_ids
                        FROM demand_products
                        GROUP BY grp
                    )
                    SELECT g.name, g.demand_count
                    FROM grouped g
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM prices pr
                        JOIN stores s ON s.id = pr.store_id
                        WHERE pr.product_id = ANY(g.product_ids)
                          AND LOWER(s.chain) = LOWER($2)
                          AND pr.collected_at > NOW() - INTERVAL '14 days'
                          AND pr.price > 0
                    )
                    ORDER BY g.demand_count DESC
                """, days, chain)
                missing_total_count = len(missing_rows_all)
                missing_rows = missing_rows_all[:8]
            except Exception as e:
                print(f"[analytics_dashboard] missing_rows query failed: {e}")
                missing_rows = []
                missing_total_count = 0

        # Hinnatundlikud kaotused — toorandmed. Arvutus (bucketid, near
        # win/loss, competitor breakdown) tehakse hiljem Python-poolel,
        # kuna JSONB payload'i sisemuses agregeerimine on SQL-is
        # ebamugav ja andmemaht on väike (üks rida iga /compare kutse
        # kohta selle perioodi jooksul).
        try:
            basket_compare_rows = await conn.fetch("""
                SELECT payload
                FROM analytics_events
                WHERE event_type = 'basket_compare'
                  AND created_at >= CURRENT_DATE - ($1::int - 1)
                  AND created_at < CURRENT_DATE + INTERVAL '1 day'
            """, days)
        except Exception as e:
            print(f"[analytics_dashboard] basket_compare_rows query failed: {e}")
            basket_compare_rows = []

        daily_rows = await conn.fetch("""
            SELECT DATE(created_at) AS day, event_type, COUNT(*) AS cnt
            FROM analytics_events
            WHERE created_at >= CURRENT_DATE - ($1::int - 1)
              AND created_at < CURRENT_DATE + INTERVAL '1 day'
              AND ($2::text IS NULL OR LOWER(chain) = LOWER($2))
            GROUP BY DATE(created_at), event_type
            ORDER BY day ASC
        """, days, chain)

        totals = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'basket_add') AS total_adds,
                COUNT(*) FILTER (WHERE event_type = 'basket_win') AS total_wins,
                COUNT(*) FILTER (WHERE event_type = 'product_view') AS total_views,
                COUNT(DISTINCT COALESCE(
                    CASE WHEN user_id IS NOT NULL THEN 'u:' || user_id::text END,
                    CASE WHEN device_key IS NOT NULL AND device_key <> '' THEN 'd:' || device_key END
                )) AS unique_visitors
            FROM analytics_events
            WHERE created_at >= CURRENT_DATE - ($1::int - 1)
              AND created_at < CURRENT_DATE + INTERVAL '1 day'
              AND ($2::text IS NULL OR LOWER(chain) = LOWER($2))
        """, days, chain)

        prev_totals = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'basket_add') AS total_adds,
                COUNT(*) FILTER (WHERE event_type = 'basket_win') AS total_wins,
                COUNT(*) FILTER (WHERE event_type = 'product_view') AS total_views,
                COUNT(DISTINCT COALESCE(
                    CASE WHEN user_id IS NOT NULL THEN 'u:' || user_id::text END,
                    CASE WHEN device_key IS NOT NULL AND device_key <> '' THEN 'd:' || device_key END
                )) AS unique_visitors
            FROM analytics_events
            WHERE created_at >= CURRENT_DATE - (($1::int * 2) - 1)
              AND created_at < CURRENT_DATE - ($1::int - 1)
              AND ($2::text IS NULL OR LOWER(chain) = LOWER($2))
        """, days, chain)

        last_event = await conn.fetchval("""
            SELECT MAX(created_at) FROM analytics_events
            WHERE ($1::text IS NULL OR LOWER(chain) = LOWER($1))
        """, chain)

    def delta_html(current, previous):
        if previous == 0:
            if current > 0:
                return '<span style="color:#1B9A59;font-size:12px;font-weight:600">Uus aktiivsus</span>'
            return ''
        pct = round((current - previous) / previous * 100, 1)
        pct_text = str(abs(pct)).replace('.', ',')
        if pct > 0:
            return f'<span style="color:#1B9A59;font-size:12px;font-weight:600">↑ {pct_text}%</span>'
        elif pct < 0:
            return f'<span style="color:#e74c3c;font-size:12px;font-weight:600">↓ {pct_text}%</span>'
        return '<span style="color:#9198A3;font-size:12px">Muutuseta</span>'

    total_adds = totals['total_adds'] or 0
    total_wins = totals['total_wins'] or 0
    total_views = totals['total_views'] or 0
    unique_visitors = totals['unique_visitors'] or 0
    prev_unique_visitors = prev_totals['unique_visitors'] or 0

    delta_adds = delta_html(total_adds, prev_totals['total_adds'] or 0)
    delta_wins = delta_html(total_wins, prev_totals['total_wins'] or 0)
    delta_views = delta_html(total_views, prev_totals['total_views'] or 0)

    # Privaatsuslävi kehtib ainult partnerivaates — admin näeb alati täpset
    # arvu, kuna admin ei ole väline osapool, kelle eest väikest valimit
    # varjata (is_admin on juba varem funktsioonis arvutatud).
    PRIVACY_THRESHOLD = 10
    should_suppress_visitors = (not is_admin) and unique_visitors < PRIVACY_THRESHOLD
    if should_suppress_visitors:
        visitors_display = "&lt; 10"
        visitors_note_html = (
            'Privaatsuslävi rakendatud'
            '<span class="info-dot" title="Loendab sisseloginud kasutajaid ja pseudonüümseid seadmetunnuseid. '
            'Sama inimene mitmes seadmes võib lugeda mitme külastajana.">i</span>'
        )
    else:
        visitors_display = f"{unique_visitors:,}"
        visitors_note_html = f'Aktiivsed külastajad {delta_html(unique_visitors, prev_unique_visitors)}'

    basket_add_rate = round(total_adds / total_views * 100, 1) if total_views > 0 else None
    win_rate = round(total_wins / (all_wins_total or 1) * 100, 1) if chain and all_wins_total else None

    if last_event:
        diff = datetime.datetime.now(datetime.timezone.utc) - last_event
        mins = int(diff.total_seconds() / 60)
        if mins < 60:
            last_event_str = f"{mins} min tagasi"
        elif mins < 1440:
            last_event_str = f"{mins // 60} tundi tagasi"
        else:
            last_event_str = last_event.strftime("%d.%m kell %H:%M")
    else:
        last_event_str = "Andmed puuduvad"

    chain_filter_label = chain.capitalize() if chain else "Kõik ketid"

    CHAINS = [("", "Kõik"), ("selver", "Selver"), ("rimi", "Rimi"), ("prisma", "Prisma"), ("coop", "Coop"), ("maxima", "Maxima")]
    if is_admin:
        chain_btns = "".join(
            f'<a class="filter-pill{"  active" if (not chain and not value) or value == chain else ""}" href="/admin/analytics?days={days}{"&chain=" + value if value else ""}">{escape(label)}</a>'
            for value, label in CHAINS
        )
    else:
        chain_btns = ""

    chain_param = f"&chain={chain}" if chain else ""
    days_btns = "".join(
        f'<a class="filter-pill{"  active" if period == days else ""}" href="/admin/analytics?days={period}{chain_param}">{period}p</a>'
        for period in [7, 14, 30, 90]
    )

    max_wins = max((r['wins'] for r in basket_wins_rows), default=0)
    total_chains = len(basket_wins_rows)

    partner_position = None
    partner_wins = 0
    if not is_admin and locked_chain:
        for r in basket_wins_rows:
            if r['chain'] and r['chain'].lower() == locked_chain.lower():
                partner_position = r['position']
                partner_wins = r['wins']
                break

    position_summary = ""
    if not is_admin and locked_chain:
        pos = partner_position or total_chains
        pos_wins = partner_wins or 0
        ties = [r for r in basket_wins_rows if r['position'] == pos]
        if len(ties) > 1:
            pos_text = f"jagab {pos}. kohta {total_chains} jaeketi seas"
        else:
            pos_text = f"on {pos}. kohal {total_chains} jaeketi seas"
        position_summary = f'''<div style="padding:16px;background:var(--accent-soft);border-radius:var(--radius-md);margin-bottom:16px;border:1px solid #FFD7A3">
            <div style="font-size:13px;color:var(--accent-dark);font-weight:700;margin-bottom:4px">Teie positsioon</div>
            <div style="font-size:20px;font-weight:800;color:var(--text)">{locked_chain.capitalize()} {pos_text}</div>
            <div style="font-size:12px;color:var(--text-secondary);margin-top:4px">Valitud perioodil {pos_wins:,} korvivõitu</div>
            <div style="font-size:11px;color:var(--text-muted);margin-top:6px">Konkurendid on anonüümitud ning tähised põhinevad valitud perioodi järjestusel.</div>
        </div>'''

    def wins_row(actual_position, row, display_name, is_partner):
        pct = round(row['wins']/max_wins*100,1) if max_wins else 0
        highlight = 'border:1px solid var(--accent);' if is_partner else ''
        name_style = 'font-weight:800;color:var(--accent-dark)' if is_partner else ''
        position_class = " ranking-position-first" if actual_position == 1 else ""
        return f"""<div class="ranking-item" style="{highlight}">
            <div class="ranking-row">
                <div class="ranking-name"><span class="ranking-position{position_class}">{actual_position}</span><span style="{name_style}">{escape(display_name)}</span></div>
                <span class="ranking-count">{row['wins']:,}<small> võitu</small></span>
            </div>
            <div class="progress-track"><span class="progress-fill" style="--bar-width:{pct}%"></span></div>
        </div>"""

    wins_rows = []
    anonymous_index = 0
    for r in basket_wins_rows:
        actual_position = r['position']
        row_chain = (r['chain'] or '').lower()
        is_partner_row = bool(not is_admin and locked_chain and row_chain == locked_chain.lower())
        if is_admin:
            display_name = r['chain'].capitalize() if r['chain'] else '—'
        elif is_partner_row:
            display_name = locked_chain.capitalize()
        else:
            anonymous_index += 1
            display_name = f"Konkurent {chr(64 + anonymous_index)}"
        wins_rows.append(wins_row(actual_position, r, display_name, is_partner_row))

    wins_html = position_summary + ("".join(wins_rows) or '<div class="empty-state">Valitud perioodi kohta ei ole veel korvi võitude andmeid.</div>')

    max_adds = max((r['adds'] for r in top_products_rows), default=0)
    products_html = "".join(
        f"""<div class="product-row">
            <span class="product-rank">{i}</span>
            <div class="product-content">
                <div class="product-name" title="{escape(r['name'] or '')}">{escape(r['name'] or f'ID {r["product_id"]}')}</div>
                <div style="font-size:11px;color:var(--text-muted);margin-bottom:6px">{escape((r['chain'] or '').capitalize())}</div>
                <div class="product-bar-track"><span class="product-bar-fill" style="--bar-width:{round(r['adds']/max_adds*100,1) if max_adds else 0}%"></span></div>
            </div>
            <div class="product-count">{r['adds']:,}<small>lisamist</small></div>
        </div>"""
        for i, r in enumerate(top_products_rows, 1)
    ) or '<div class="empty-state">Valitud perioodi kohta ei ole veel toodete lisamise andmeid.</div>'

    max_category = max((r['cnt'] for r in category_rows), default=0)
    category_html = "".join(
        f"""<div class="product-row">
            <span class="product-rank">{i}</span>
            <div class="product-content">
                <div class="product-name">{escape(r['category'])}</div>
                <div class="product-bar-track"><span class="product-bar-fill" style="--bar-width:{round(r['cnt']/max_category*100,1) if max_category else 0}%"></span></div>
            </div>
            <div class="product-count">{r['cnt']:,}<small>lisamist</small></div>
        </div>"""
        for i, r in enumerate(category_rows, 1)
    ) or '<div class="empty-state">Valitud perioodi kohta ei ole veel kategooria-andmeid.</div>'

    MIN_CURRENT_FOR_MOMENTUM = 3
    MIN_PREVIOUS_FOR_PERCENT = 2
    category_momentum_list = []
    for r in category_momentum_rows:
        cur = r["current_cnt"] or 0
        prev = r["prev_cnt"] or 0
        if cur < MIN_CURRENT_FOR_MOMENTUM:
            continue
        if prev == 0:
            category_momentum_list.append((r["category"], cur, None))
        elif prev >= MIN_PREVIOUS_FOR_PERCENT:
            growth = round((cur - prev) / prev * 100, 1)
            if growth > 0:
                category_momentum_list.append((r["category"], cur, growth))
    category_momentum_list = sorted(
        category_momentum_list,
        key=lambda x: (
            x[1],                              # praegune maht
            x[2] if x[2] is not None else 999  # kasv/uudsus
        ),
        reverse=True,
    )[:5]
    category_momentum_html = "".join(
        f"""<div class="product-row">
            <span class="product-rank">{i}</span>
            <div class="product-content">
                <div class="product-name">{escape(name)}</div>
            </div>
            <div class="product-count" style="color:{'#1B9A59' if growth is not None else 'var(--accent-dark)'}">
                {f"↑ {str(growth).replace('.', ',')}% · {cur} lisamist" if growth is not None else f"Uus · {cur} lisamist"}
            </div>
        </div>"""
        for i, (name, cur, growth) in enumerate(category_momentum_list, 1)
    ) or '<div class="empty-state">Valitud perioodil ei tuvastatud kasvavaid kategooriaid.</div>'

    if not chain:
        missing_html = '<div class="empty-state">Vali jaekett, et näha puuduvat sortimenti.</div>'
    else:
        max_demand = max((r['demand_count'] for r in missing_rows), default=0)
        if missing_total_count > 0:
            top3_names = ", ".join(escape(r["name"] or "—") for r in missing_rows[:3])
            missing_summary_html = f"""<div class="missing-summary">
                <strong>{missing_total_count} toodet</strong>, mille vastu kasutajad huvi näitasid, kuid mille aktiivset hinda või sortimendi vastet teie ketis Seivy andmetes ei leitud.
                <div class="missing-summary-top">Top puuduv nõudlus: {top3_names}</div>
            </div>"""
        else:
            missing_summary_html = ""
        missing_html = missing_summary_html + ("".join(
            f"""<div class="product-row">
                <span class="product-rank">{i}</span>
                <div class="product-content">
                    <div class="product-name" title="{escape(r['name'] or '')}">{escape(r['name'] or '—')}</div>
                    <div class="product-bar-track"><span class="product-bar-fill" style="--bar-width:{round(r['demand_count']/max_demand*100,1) if max_demand else 0}%"></span></div>
                </div>
                <div class="product-count">{r['demand_count']:,}<small>nõudlust</small></div>
            </div>"""
            for i, r in enumerate(missing_rows, 1)
        ) or '<div class="empty-state">Puuduvat sortimenti ei tuvastatud valitud perioodil.</div>')

    # --- Hinnatundlikud kaotused ---
    # V1 spec (ChatGPT design consult, juuli 2026): peamine KPI on
    # "near_losses_under_1eur" — kaotused, kus valitud kett jäi
    # võitjast alla 1€ vahega maha. Sekundaarne mõõdik on "ohustatud
    # võidud" (near-wins) — võidud, kus lähim konkurent oli alla 1€
    # kaugusel, et vältida partneri enesepettust "me võitsime palju"
    # stiilis, kui võidud olid tegelikult haprad.
    #
    # Kaks miinimumi kaitsevad väikese valimi väärtõlgendamise eest:
    # kogu paneel vajab vähemalt 30 sobivat (eligible) basket_compare
    # sündmust perioodis, ja valitud kett vajab vähemalt 10 võrdlust,
    # kus ta osales, enne kui talle midagi näidatakse. "Trendina" kõlav
    # tekst (kollane callout) tuleb alles 3+ napi kaotuse juures —
    # sama põhimõte, mis MIN_CURRENT_FOR_MOMENTUM=3 mujal dashboardil.
    MIN_BASKET_COMPARE_FOR_PRICE_SENSITIVITY = 30
    MIN_CHAIN_COMPARE_FOR_PRICE_SENSITIVITY = 10
    MIN_NEAR_LOSSES_TO_HIGHLIGHT = 3

    if not chain:
        price_sensitivity_html = '<div class="empty-state">Vali jaekett, et näha hinnatundlikke kaotusi.</div>'
    else:
        eligible_compares = []
        for r in basket_compare_rows:
            raw_payload = r["payload"]
            try:
                payload = raw_payload if isinstance(raw_payload, dict) else json.loads(raw_payload)
            except Exception:
                continue
            raw_totals = payload.get("chain_totals") or {}
            cheapest_chain_val = payload.get("cheapest_chain")
            cheapest_total_val = payload.get("cheapest_total")
            if cheapest_chain_val is None or cheapest_total_val is None:
                continue
            try:
                normalized_totals = {
                    str(k).lower().strip(): float(v)
                    for k, v in raw_totals.items()
                    if k is not None and v is not None
                }
                cheapest_total_float = float(cheapest_total_val)
            except Exception:
                continue
            if len(normalized_totals) < 2:
                continue
            eligible_compares.append({
                "chain_totals": normalized_totals,
                "cheapest_chain": str(cheapest_chain_val).lower().strip(),
                "cheapest_total": cheapest_total_float,
            })

        chain_lower = chain.lower()

        if len(eligible_compares) < MIN_BASKET_COMPARE_FOR_PRICE_SENSITIVITY:
            price_sensitivity_html = '<div class="empty-state">Hinnatundlike kaotuste kuvamiseks kogume veel andmeid.</div>'
        else:
            chain_events = [e for e in eligible_compares if chain_lower in e["chain_totals"]]
            chain_compare_count = len(chain_events)

            if chain_compare_count < MIN_CHAIN_COMPARE_FOR_PRICE_SENSITIVITY:
                price_sensitivity_html = '<div class="empty-state">Selle keti kohta pole perioodis veel piisavalt võrdlusi.</div>'
            else:
                win_count = 0
                near_loss_050 = near_loss_100 = near_loss_200 = 0
                near_win_050 = near_win_100 = near_win_200 = 0
                lost_to_counter: dict = {}

                for e in chain_events:
                    ct = e["chain_totals"]
                    own_total = ct[chain_lower]
                    if e["cheapest_chain"] == chain_lower:
                        win_count += 1
                        others = [v for k, v in ct.items() if k != chain_lower]
                        if others:
                            gap = min(others) - own_total
                            if 0 < gap <= 0.50:
                                near_win_050 += 1
                            elif 0.50 < gap <= 1.00:
                                near_win_100 += 1
                            elif 1.00 < gap <= 2.00:
                                near_win_200 += 1
                    else:
                        gap = own_total - e["cheapest_total"]
                        if 0 < gap <= 0.50:
                            near_loss_050 += 1
                            lost_to_counter[e["cheapest_chain"]] = lost_to_counter.get(e["cheapest_chain"], 0) + 1
                        elif 0.50 < gap <= 1.00:
                            near_loss_100 += 1
                            lost_to_counter[e["cheapest_chain"]] = lost_to_counter.get(e["cheapest_chain"], 0) + 1
                        elif 1.00 < gap <= 2.00:
                            near_loss_200 += 1

                near_losses_under_1eur = near_loss_050 + near_loss_100
                near_wins_under_1eur = near_win_050 + near_win_100

                top_lost_to = sorted(lost_to_counter.items(), key=lambda x: -x[1])[:3]
                top_lost_html = "".join(
                    f"""<div class="sensitivity-row">
                        <div class="product-content"><div class="product-name">{escape(c.capitalize() if is_admin else f"Konkurent {chr(64 + i)}")}</div></div>
                        <div class="product-count">{n:,}<small>korda</small></div>
                    </div>"""
                    for i, (c, n) in enumerate(top_lost_to, 1)
                ) or '<div style="font-size:13px;color:var(--text-secondary);padding:8px 0">Alla 1 € kaotusi ei tuvastatud.</div>'

                callout_html = ""
                if near_losses_under_1eur >= MIN_NEAR_LOSSES_TO_HIGHLIGHT:
                    callout_html = f"""<div style="padding:14px 16px;margin:4px 0 14px;border-radius:var(--radius-md);background:var(--accent-soft);border:1px solid #FFD7A3;font-size:13px;color:var(--text);line-height:1.5">
                        Kui korv oleks olnud kuni <b>1 €</b> odavam, oleks {escape(chain.capitalize())} võitnud veel <b>{near_losses_under_1eur}</b> lisavõrdlust selle perioodi jooksul.
                    </div>"""

                price_sensitivity_html = f"""
                    <p class="metric-value" style="font-size:32px;margin:0 0 2px">{near_losses_under_1eur:,}</p>
                    <p class="metric-note" style="margin-bottom:12px">kaotust alla 1 € vahega</p>
                    {callout_html}
                    <div class="product-list">
                        <div class="sensitivity-row"><div class="product-content"><div class="product-name">Väga napp &lt;0.50 €</div></div><div class="product-count">{near_loss_050:,}</div></div>
                        <div class="sensitivity-row"><div class="product-content"><div class="product-name">Napp 0.50–1.00 €</div></div><div class="product-count">{near_loss_100:,}</div></div>
                        <div class="sensitivity-row"><div class="product-content"><div class="product-name">Võidetav 1.00–2.00 €</div></div><div class="product-count">{near_loss_200:,}</div></div>
                    </div>
                    <div style="margin-top:14px;padding-top:14px;border-top:1px solid var(--border)">
                        <div class="panel-description" style="margin-bottom:8px">Enim kaotati napilt (alla 1 €):</div>
                        <div class="product-list">{top_lost_html}</div>
                    </div>
                    <div style="margin-top:14px;padding-top:14px;border-top:1px solid var(--border);font-size:13px;color:var(--text-secondary)">
                        Ohustatud võidud alla 1 €: <b style="color:var(--text)">{near_wins_under_1eur:,}</b> ({win_count:,} võidust)
                    </div>
                """

    daily_dict = {}
    for r in daily_rows:
        d = str(r['day'])
        if d not in daily_dict:
            daily_dict[d] = {}
        daily_dict[d][r['event_type']] = r['cnt']

    today = datetime.datetime.now(datetime.timezone.utc).date()
    all_days = [today - datetime.timedelta(days=offset) for offset in range(days - 1, -1, -1)]
    daily_labels_json = json.dumps([d.strftime("%m-%d") for d in all_days], ensure_ascii=False)
    daily_adds_json = json.dumps([daily_dict.get(str(d), {}).get('basket_add', 0) for d in all_days])
    daily_wins_json = json.dumps([daily_dict.get(str(d), {}).get('basket_win', 0) for d in all_days])
    sorted_days = [str(d) for d in all_days]

    chart_height = 250 if len(sorted_days) <= 3 else 355

    active_days = sum(
        1 for day in all_days
        if daily_dict.get(str(day), {}).get("basket_add", 0) > 0
        or daily_dict.get(str(day), {}).get("basket_win", 0) > 0
    )
    chart_summary = (
        '<p style="color:var(--text-secondary);font-size:13px;margin-top:8px">'
        'Valitud perioodil toimus aktiivsus ühel päeval.'
        '</p>'
    ) if active_days == 1 else ""

    rates_html = ""
    if basket_add_rate is not None:
        rates_html += f'<div style="font-size:12px;color:var(--text-secondary);margin-top:6px">Korvi lisamise määr: <b style="color:var(--text)">{basket_add_rate}%</b> vaatamistest</div>'
    if win_rate is not None:
        rates_html += f'<div style="font-size:12px;color:var(--text-secondary);margin-top:4px">Osakaal kõigist korvivõitudest: <b style="color:var(--text)">{win_rate}%</b></div>'

    title = f"{chain.capitalize() if chain else 'Kõik ketid'} — viimased {days} päeva"

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
.topbar-context {{ display: flex; align-items: center; gap: 10px; color: var(--text-secondary); font-size: 13px; font-weight: 600; }}
.live-dot {{ width: 8px; height: 8px; border-radius: 999px; background: var(--success); box-shadow: 0 0 0 4px var(--success-soft); }}
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
.metric-note {{ position: relative; z-index: 1; margin: 10px 0 0; color: var(--text-muted); font-size: 12px; font-weight: 550; display: flex; align-items: center; flex-wrap: wrap; gap: 4px; }}
.info-dot {{ display: inline-grid; place-items: center; width: 16px; height: 16px; margin-left: 5px; border-radius: 999px; background: var(--surface-muted); color: var(--text-muted); font-size: 10px; font-weight: 800; cursor: help; }}
.privacy-badge {{ display: inline-flex; align-items: center; gap: 5px; margin-top: 8px; padding: 5px 8px; border-radius: 999px; background: #F3F5F8; color: var(--text-secondary); font-size: 11px; font-weight: 650; }}
.dashboard-grid {{ display: grid; grid-template-columns: minmax(0,1.6fr) minmax(330px,.8fr); gap: 22px; }}
.dashboard-column {{ display: flex; flex-direction: column; gap: 22px; }}
.panel {{ padding: 24px; border: 1px solid var(--border); border-radius: var(--radius-lg); background: var(--surface); box-shadow: var(--shadow); }}
.panel-header {{ display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 23px; gap: 18px; }}
.panel-title {{ margin: 0; color: var(--text); font-size: 17px; font-weight: 730; letter-spacing: -.025em; }}
.panel-description {{ margin: 6px 0 0; color: var(--text-secondary); font-size: 12px; line-height: 1.5; }}
.panel-badge {{ padding: 7px 10px; border-radius: 999px; background: var(--surface-muted); color: var(--text-secondary); font-size: 11px; font-weight: 700; }}
.chart-wrapper {{ position: relative; width: 100%; height: {chart_height}px; }}
.chart-wrapper.compact {{ height: 320px; }}
.ranking-list {{ display: flex; flex-direction: column; gap: 11px; }}
.ranking-item {{ padding: 14px; border: 1px solid var(--border); border-radius: var(--radius-md); background: var(--surface-muted); }}
.ranking-row {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; gap: 12px; }}
.ranking-name {{ display: flex; align-items: center; gap: 10px; color: var(--text); font-size: 13px; font-weight: 680; }}
.ranking-position {{ display: grid; width: 26px; height: 26px; place-items: center; border-radius: 8px; background: #fff; color: var(--text-secondary); font-size: 11px; font-weight: 800; box-shadow: 0 1px 3px rgba(20,24,32,.07); }}
.ranking-position-first {{ background: var(--accent); color: #fff; }}
.ranking-count {{ color: var(--text); font-size: 13px; font-weight: 750; }}
.ranking-count small {{ color: var(--text-muted); font-size: 10px; font-weight: 600; }}
.progress-track {{ width: 100%; height: 7px; overflow: hidden; border-radius: 999px; background: #E8EBF0; }}
.progress-fill {{ display: block; width: var(--bar-width,0%); height: 100%; border-radius: inherit; background: linear-gradient(90deg,var(--accent),#FFB343); }}
.product-list {{ display: flex; flex-direction: column; gap: 2px; }}
.product-row {{ display: grid; grid-template-columns: 32px minmax(0,1fr) auto; align-items: center; padding: 13px 4px; border-bottom: 1px solid var(--border); gap: 12px; }}
.product-row:last-child {{ border-bottom: 0; }}
.sensitivity-row {{ display: grid; grid-template-columns: minmax(0,1fr) auto; align-items: center; padding: 13px 4px; border-bottom: 1px solid var(--border); gap: 12px; }}
.sensitivity-row:last-child {{ border-bottom: 0; }}
.product-rank {{ display: grid; width: 28px; height: 28px; place-items: center; border-radius: 8px; background: var(--surface-muted); color: var(--text-secondary); font-size: 11px; font-weight: 750; }}
.product-row:first-child .product-rank {{ background: var(--accent-soft); color: var(--accent-dark); }}
.product-name {{ margin-bottom: 4px; overflow: hidden; color: var(--text); font-size: 13px; font-weight: 650; line-height: 1.35; text-overflow: ellipsis; white-space: nowrap; }}
.product-bar-track {{ width: 100%; height: 5px; overflow: hidden; border-radius: 999px; background: #ECEFF3; }}
.product-bar-fill {{ display: block; width: var(--bar-width,0%); height: 100%; border-radius: inherit; background: var(--accent); }}
.product-count {{ min-width: 58px; color: var(--text); font-size: 13px; font-weight: 750; text-align: right; }}
.product-count small {{ display: block; margin-top: 2px; color: var(--text-muted); font-size: 9px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }}
.missing-summary {{ padding: 14px 16px; margin-bottom: 14px; border-radius: var(--radius-md); background: var(--accent-soft); border: 1px solid #FFD7A3; font-size: 13px; color: var(--text); }}
.missing-summary-top {{ margin-top: 6px; font-size: 12px; color: var(--text-secondary); }}
.empty-state {{ display: grid; min-height: 180px; place-items: center; padding: 24px; border: 1px dashed var(--border-strong); border-radius: var(--radius-md); background: var(--surface-muted); color: var(--text-secondary); font-size: 13px; text-align: center; }}
.footer {{ display: flex; align-items: center; justify-content: space-between; margin-top: 24px; padding: 0 4px; gap: 20px; color: var(--text-muted); font-size: 11px; }}
.footer-brand {{ color: var(--text-secondary); font-weight: 700; }}
.export-btn {{ display: inline-flex; align-items: center; gap: 6px; padding: 8px 14px; border: 1px solid var(--border); border-radius: 999px; background: var(--surface); color: var(--text-secondary); font-size: 12px; font-weight: 650; text-decoration: none; transition: border-color 160ms,background 160ms; }}
.export-btn:hover {{ border-color: var(--accent); color: var(--accent-dark); background: var(--accent-softer); }}
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
      Viimane sündmus: {last_event_str}
      {"&nbsp;·&nbsp;<a href='/admin/analytics/logout' style='color:var(--text-secondary);font-size:12px;text-decoration:none'>Logi välja</a>" if not is_admin else ""}
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
      {rates_html}
    </div>
    <div style="display:flex;flex-direction:column;align-items:flex-end;gap:10px">
      <div class="period-summary"><span>Analüüsiperiood</span><strong>Viimased {days} päeva</strong></div>
      <a class="export-btn" href="/admin/analytics/export?days={days}{chain_param}">⬇ Ekspordi CSV</a>
    </div>
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
      <p class="metric-note">Toodete lisamised korvidesse {delta_adds}</p>
    </article>
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Korvi võidud</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3h8v4a4 4 0 0 1-8 0V3Z"></path><path d="M6 5H3v1a5 5 0 0 0 5 5"></path><path d="M18 5h3v1a5 5 0 0 1-5 5"></path><path d="M12 11v5"></path><path d="M8 21h8"></path><path d="M10 16h4v5h-4z"></path></svg></span>
      </div>
      <p class="metric-value">{total_wins:,}</p>
      <p class="metric-note">Soodsaima korvi tulemused {delta_wins}</p>
    </article>
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Toote vaatamised</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z"></path><circle cx="12" cy="12" r="2.5"></circle></svg></span>
      </div>
      <p class="metric-value">{total_views:,}</p>
      <p class="metric-note">Tootekaartide avamised {delta_views}</p>
    </article>
    <article class="metric-card">
      <div class="metric-top">
        <span class="metric-label">Unikaalsed külastajad</span>
        <span class="metric-icon"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="8" r="3"></circle><path d="M3 20a6 6 0 0 1 12 0"></path><circle cx="17" cy="9" r="2"></circle><path d="M15.5 15.5A5 5 0 0 1 21 20"></path></svg></span>
      </div>
      <p class="metric-value">{visitors_display}</p>
      <p class="metric-note">{visitors_note_html}</p>
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
          <canvas id="dailyActivityChart"></canvas>
        </div>
        {chart_summary}
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Enim korvi lisatud tooted</h2>
            <p class="panel-description">Tooted, mille vastu kasutajad on kõige suuremat ostuhuvi näidanud.</p>
          </div>
          <span class="panel-badge">Top 10</span>
        </div>
        <div class="product-list">{products_html}</div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Kaotatud nõudlus</h2>
            <p class="panel-description">Tooted, mille vastu kasutajad huvi näitasid, kuid mille aktiivset hinda või sortimendi vastet selles ketis Seivy andmetes ei leitud. Nõudlus = toote vaatamised + korvi lisamised.</p>
          </div>
        </div>
        <div class="product-list">{missing_html}</div>
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
            <h2 class="panel-title">Hinnatundlikud kaotused</h2>
            <p class="panel-description">Korvid, kus valitud kett jäi võitjast väikese hinnavahega maha.</p>
          </div>
        </div>
        {price_sensitivity_html}
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Kategooria jaotus</h2>
            <p class="panel-description">Millised kategooriad toovad enim korvi lisamisi.</p>
          </div>
        </div>
        <div class="product-list">{category_html}</div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Kiiremini kasvavad kategooriad</h2>
            <p class="panel-description">Võrreldes eelmise sama pika perioodiga.</p>
          </div>
        </div>
        <div class="product-list">{category_momentum_html}</div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Aktiivsuse trend</h2>
            <p class="panel-description">Korvi lisamised ja võidud samal ajaskaalal.</p>
          </div>
        </div>
        <div class="chart-wrapper compact">
          <canvas id="activityOverviewChart"></canvas>
        </div>
      </article>
    </div>
  </section>

  <footer class="footer">
    <span>Näitajad põhinevad Seivy rakenduses kogutud koondatud kasutussündmustel. Unikaalsed külastajad arvestavad sisselogitud kasutajaid ja pseudonüümseid seadmetunnuseid.</span>
    <span class="footer-brand">Seivy partneranalüütika{"&nbsp;·&nbsp;<a href='/' style='color:inherit'>← Admin</a>" if is_admin else ""}</span>
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
    resp = HTMLResponse(html)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers["Cache-Control"] = "private, no-store"
    return resp


@router.get("/admin/analytics/logout")
async def analytics_logout():
    response = HTMLResponse("""<!DOCTYPE html>
<html lang="et">
<head><meta charset="UTF-8"><title>Välja logitud</title>
<style>body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:60px 20px;text-align:center;background:#F5F6F8;color:#171A1F}
h2{font-size:24px;font-weight:700;margin-bottom:12px}p{color:#68707D;font-size:15px}</style></head>
<body>
<div style="max-width:480px;margin:0 auto;background:#fff;padding:40px;border-radius:20px;box-shadow:0 1px 2px rgba(20,24,32,.04),0 8px 24px rgba(20,24,32,.055)">
  <div style="font-size:48px;margin-bottom:16px">👋</div>
  <h2>Olete välja logitud</h2>
  <p>Uuesti sisenemiseks avage teile saadetud turvaline ligipääsulink.</p>
</div>
</body></html>""")
    response.delete_cookie("seivy_analytics_token", path="/admin/analytics")
    return response


async def _export_brand_csv(conn, partner_name: str, brand_filter: list, days: int):
    """Builds the brand/producer CSV export: one row per product group,
    with views/adds/add-rate plus the per-chain price pivot and a list
    of chains where no active price was found. Mirrors the same
    brand_filter resolution and active-price filtering used in
    _render_brand_dashboard, so the CSV and the on-screen dashboard
    never disagree.
    """
    import csv, io
    from fastapi.responses import StreamingResponse

    ALLOWED_DAYS = {7, 14, 30, 90}
    if days not in ALLOWED_DAYS:
        days = 30

    CHAINS_ORDER = ["selver", "rimi", "prisma", "coop", "maxima"]
    header = ["Toode", "Vaatamised", "Korvi lisamised", "Lisamise määr"] + \
        [f"{ch.capitalize()} hind" for ch in CHAINS_ORDER] + ["Puuduvad ketid"]

    def _csv_response(rows):
        output = io.StringIO()
        writer = csv.writer(output, delimiter=";")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
        csv_content = "\ufeff" + output.getvalue()
        safe_partner = "".join(
            c.lower() if c.isalnum() else "_"
            for c in (partner_name or "tootja")
        ).strip("_")[:40] or "tootja"
        filename = f"seivy_tootja_{safe_partner}_{days}p.csv"
        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    brand_filter_lower = [b.lower() for b in brand_filter]

    product_id_rows = await conn.fetch("""
        SELECT pgm.product_id, pgm.group_id
        FROM product_group_members pgm
        JOIN product_groups pg ON pg.id = pgm.group_id
        WHERE LOWER(pg.brand) = ANY($1::text[])
    """, brand_filter_lower)
    brand_product_ids = [r["product_id"] for r in product_id_rows]
    brand_group_ids = list({r["group_id"] for r in product_id_rows})
    group_by_product = {r["product_id"]: r["group_id"] for r in product_id_rows}

    if not brand_product_ids:
        return _csv_response([])

    group_names_rows = await conn.fetch("""
        SELECT id AS group_id, COALESCE(canonical_name, 'Toode #' || id) AS name
        FROM product_groups WHERE id = ANY($1::int[])
    """, brand_group_ids)
    group_names = {r["group_id"]: r["name"] for r in group_names_rows}

    demand_rows = await conn.fetch("""
        SELECT a.product_id,
            COUNT(*) FILTER (WHERE a.event_type = 'product_view') AS views,
            COUNT(*) FILTER (WHERE a.event_type = 'basket_add') AS adds
        FROM analytics_events a
        WHERE a.product_id = ANY($1::int[])
          AND a.created_at >= CURRENT_DATE - ($2::int - 1)
          AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
        GROUP BY a.product_id
    """, brand_product_ids, days)

    group_demand: dict = {}
    for r in demand_rows:
        gid = group_by_product.get(r["product_id"])
        if gid is None:
            continue
        entry = group_demand.setdefault(gid, {"views": 0, "adds": 0})
        entry["views"] += r["views"] or 0
        entry["adds"] += r["adds"] or 0

    # Same active-price filter as the dashboard: only prices collected
    # in the last 14 days, and promo_price=0 treated as "no promo".
    price_rows = await conn.fetch("""
        SELECT
            pgm.group_id,
            LOWER(s.chain) AS chain,
            MIN(COALESCE(NULLIF(pr.promo_price, 0), pr.price)) AS price
        FROM product_group_members pgm
        JOIN prices pr ON pr.product_id = pgm.product_id
        JOIN stores s ON s.id = pr.store_id
        WHERE pgm.group_id = ANY($1::int[])
          AND pr.collected_at > NOW() - INTERVAL '14 days'
          AND pr.price > 0
        GROUP BY pgm.group_id, LOWER(s.chain)
    """, brand_group_ids)

    group_prices: dict = {}
    for r in price_rows:
        group_prices.setdefault(r["group_id"], {})[r["chain"]] = float(r["price"])

    rows_out = []
    for gid in brand_group_ids:
        name = group_names.get(gid, f"Grupp #{gid}")
        demand = group_demand.get(gid, {"views": 0, "adds": 0})
        views = demand["views"]
        adds = demand["adds"]
        add_rate = f"{round(adds / views * 100, 1)}%".replace(".", ",") if views > 0 else ""
        prices = group_prices.get(gid, {})
        price_cells = []
        missing = []
        for ch in CHAINS_ORDER:
            p = prices.get(ch)
            if p is None:
                price_cells.append("")
                missing.append(ch.capitalize())
            else:
                price_cells.append(f"{p:.2f}".replace(".", ","))
        rows_out.append([name, views, adds, add_rate, *price_cells, ", ".join(missing)])

    return _csv_response(rows_out)


@router.get("/admin/analytics/export")
async def analytics_export(request: Request, days: int = 30, chain: str = None):
    """CSV eksport. Jaeketi/admin vaates sündmuste päevane jaotus;
    tootja (brand) vaates brändi toodete kokkuvõte (vt _export_brand_csv).
    """
    import csv, io, os
    from fastapi.responses import StreamingResponse

    ALLOWED_DAYS = {7, 14, 30, 90}
    if days not in ALLOWED_DAYS:
        days = 30

    COOKIE_NAME = "seivy_analytics_token"
    cookie_token = request.cookies.get(COOKIE_NAME)
    if not cookie_token:
        return HTMLResponse("<h2>Ligipääs keelatud.</h2>", status_code=403)

    TOKEN_MAP = {
        os.environ.get("ANALYTICS_TOKEN_SELVER", ""): "selver",
        os.environ.get("ANALYTICS_TOKEN_RIMI", ""): "rimi",
        os.environ.get("ANALYTICS_TOKEN_PRISMA", ""): "prisma",
        os.environ.get("ANALYTICS_TOKEN_COOP", ""): "coop",
        os.environ.get("ANALYTICS_TOKEN_MAXIMA", ""): "maxima",
    }
    TOKEN_MAP.pop("", None)
    admin_token = os.environ.get("ANALYTICS_TOKEN_ADMIN", "")

    is_admin = admin_token and cookie_token == admin_token
    brand_name = None
    brand_filter = None

    db = getattr(request.app.state, "db", None)

    if not is_admin and cookie_token in TOKEN_MAP:
        chain = TOKEN_MAP[cookie_token]
    elif not is_admin:
        # Not a legacy env-var token — check analytics_partners for a
        # brand token (or a newer retailer token added via /admin/partners
        # instead of an env var), same fallback as analytics_dashboard.
        if db is None:
            return HTMLResponse("<h2>Andmebaas ei ole veel valmis.</h2>", status_code=503)
        async with db.acquire() as conn:
            partner_row = await conn.fetchrow("""
                SELECT partner_type, name, brand_filter, chain_filter
                FROM analytics_partners
                WHERE token = $1
            """, cookie_token)
        if not partner_row:
            return HTMLResponse("<h2>Ligipääs keelatud.</h2>", status_code=403)
        if partner_row["partner_type"] == "retailer":
            chain = (partner_row["chain_filter"] or "").lower().strip() or None
            if not chain:
                return HTMLResponse("<h2>Partnerile pole ketti määratud.</h2>", status_code=500)
        elif partner_row["partner_type"] == "brand":
            brand_name = partner_row["name"]
            brand_filter = partner_row["brand_filter"] or []
            if not brand_filter:
                return HTMLResponse("<h2>Partnerile pole brändi määratud.</h2>", status_code=500)
        else:
            return HTMLResponse("<h2>Ligipääs keelatud.</h2>", status_code=403)

    allowed_chains_csv = {"selver", "rimi", "prisma", "coop", "maxima"}
    if chain:
        chain = chain.lower().strip()
        if chain not in allowed_chains_csv:
            chain = None

    if db is None:
        db = getattr(request.app.state, "db", None)
    if db is None:
        return HTMLResponse("<h2>Andmebaas ei ole veel valmis.</h2>", status_code=503)

    if brand_filter is not None:
        async with db.acquire() as conn:
            return await _export_brand_csv(conn, brand_name, brand_filter, days)

    async with db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                DATE(a.created_at) AS day,
                a.event_type,
                a.chain,
                p.name AS product_name,
                COUNT(*) AS count
            FROM analytics_events a
            LEFT JOIN products p ON p.id = a.product_id
            WHERE a.created_at >= CURRENT_DATE - ($1::int - 1)
              AND a.created_at < CURRENT_DATE + INTERVAL '1 day'
              AND ($2::text IS NULL OR LOWER(a.chain) = LOWER($2))
            GROUP BY DATE(a.created_at), a.event_type, a.chain, p.name
            ORDER BY day DESC, count DESC
        """, days, chain)

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["Kuupäev", "Sündmus", "Kett", "Toode", "Arv"])
    for r in rows:
        writer.writerow([r["day"], r["event_type"], r["chain"] or "", r["product_name"] or "", r["count"]])

    csv_content = "\ufeff" + output.getvalue()
    filename = f"seivy_analytics_{chain or 'koik'}_{days}p.csv"
    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

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

        import re as _re
        safe_base = _re.sub(r"[^a-zA-Z0-9_-]+", "_", product).strip("._-")[:120] or "product_image"
        allowed_extensions = {".jpg", ".jpeg", ".png", ".webp"}
        allowed_content_types = {"image/jpeg", "image/png", "image/webp"}
        ext = os.path.splitext(image.filename or "")[1].lower()
        if ext not in allowed_extensions:
            raise HTTPException(400, "Unsupported image format. Use jpg, png or webp.")
        if image.content_type and image.content_type not in allowed_content_types:
            raise HTTPException(400, "Unsupported image content type.")
        filename = f"{safe_base}{ext}"

        file_path = os.path.join(IMAGES_DIR, filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(image.file, buffer)

        if os.path.getsize(file_path) > MAX_UPLOAD_MB * 1024 * 1024:
            os.remove(file_path)
            raise HTTPException(status_code=413, detail=f"Image too large (>{MAX_UPLOAD_MB}MB)")

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
            from html import escape as _esc
            safe_product_html = _esc(product)
            safe_image_url_html = _esc(image_url, quote=True)
            return HTMLResponse(f"""
                <h2>✅ Image uploaded</h2>
                <p><b>Product:</b> {safe_product_html}</p>
                <p><b>Rows updated:</b> {updated_rows}</p>
                <p><img src="{safe_image_url_html}" alt="{safe_product_html}" style="max-width:520px;height:auto;border:1px solid #eee"/></p>
                <p><a href="/">← Back to Missing Product Images</a></p>
            """)

        return JSONResponse({"status": "success", "product": product, "image_url": image_url,
                             "rows_updated": updated_rows}, status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        if wants_html(request):
            from html import escape as _esc
            return HTMLResponse(f"<h2>❌ Upload failed</h2><pre>{_esc(str(e))}</pre><p><a href='/'>← Back</a></p>", status_code=500)
        raise
