# admin/image_gallery.py  (with R2 sanity check + sorting)
from fastapi import APIRouter, Query, Request, Depends
from fastapi.responses import HTMLResponse
from typing import Optional
from settings import get_db_pool, ADMIN_IP_ALLOWLIST

router = APIRouter(prefix="/admin", tags=["Admin"])

def _ip_allowed(request: Request) -> None:
    if not ADMIN_IP_ALLOWLIST:
        return
    client_ip = (request.client.host if request.client else "") or ""
    allowed = {ip.strip() for ip in ADMIN_IP_ALLOWLIST.split(",") if ip.strip()}
    if client_ip not in allowed:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not found")

@router.get("/images", response_class=HTMLResponse)
async def admin_image_gallery(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    missing: int = Query(0, description="1 = show rows missing image_url"),
    q: Optional[str] = Query(None, description="Search in product_name / brand"),
    sort: str = Query("newest", description="Sort order: newest or oldest"),
    _=Depends(_ip_allowed),
    pool=Depends(get_db_pool),
):
    # WHERE conditions
    where = []
    args = []
    if missing == 1:
        where.append("(image_url IS NULL OR image_url = '')")
    else:
        where.append("(image_url IS NOT NULL AND image_url <> '')")

    if q:
        where.append("(LOWER(product_name) LIKE $%d OR LOWER(brand) LIKE $%d)" % (len(args)+1, len(args)+2))
        args.extend([f"%{q.lower()}%", f"%{q.lower()}%"])

    order_clause = "DESC" if sort == "newest" else "ASC"

    sql = f"""
    SELECT id, ean, product_name, brand, amount, image_url, source_url, last_seen_utc
    FROM products
    WHERE {' AND '.join(where)}
    ORDER BY last_seen_utc {order_clause}
    LIMIT ${len(args)+1}
    """
    args.append(limit)

    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)

    items = []
    for r in rows:
        id_ = r["id"]
        ean = r["ean"] or ""
        name = (r["product_name"] or "").replace("<", "&lt;").replace(">", "&gt;")
        brand = r["brand"] or ""
        amt = r["amount"] or ""
        img = r["image_url"] or ""
        src = r["source_url"] or "#"
        seen = r["last_seen_utc"].strftime("%Y-%m-%d %H:%M") if r["last_seen_utc"] else ""

        if missing == 1:
            thumb = f"""
            <div class="card missing">
              <div class="meta"><a href="{src}" target="_blank">#{id_}</a> • {seen}</div>
              <div class="name">{name}</div>
              <div class="sub">{brand} {amt} • EAN {ean}</div>
              <div class="noimg">No image</div>
            </div>
            """
        else:
            thumb = f"""
            <div class="card">
              <a href="{img}" target="_blank">
                <img src="{img}" referrerpolicy="no-referrer" loading="lazy"
                     onerror="this.classList.add('broken'); window.brokenCount++;" />
              </a>
              <div class="name">{name}</div>
              <div class="sub">{brand} {amt} • EAN {ean}</div>
              <div class="meta"><a href="{src}" target="_blank">#{id_}</a> • {seen}</div>
            </div>
            """
        items.append(thumb)

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Admin • Image Gallery</title>
  <style>
    :root {{
      --bg:#0f1115; --fg:#e8e8ea; --muted:#9aa0a6; --card:#161922; --border:#1f2430;
    }}
    body {{ margin:0; padding:20px; font-family: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Inter,Arial;
      background:var(--bg); color:var(--fg); }}
    header {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-bottom:16px; }}
    header a, header span {{ color:var(--muted); text-decoration:none; margin-right:12px; }}
    form input, form select {{
      background:var(--card); color:var(--fg); border:1px solid var(--border);
      padding:8px 10px; border-radius:8px; outline:none;
    }}
    form button {{
      padding:8px 12px; border-radius:8px; border:1px solid var(--border);
      background:#1b2030; color:#fff; cursor:pointer;
    }}
    .grid {{
      display:grid; grid-template-columns: repeat(auto-fill, minmax(220px,1fr));
      gap:14px;
    }}
    .card {{
      background:var(--card); border:1px solid var(--border); border-radius:12px; padding:10px;
      display:flex; flex-direction:column; gap:8px;
    }}
    .card.missing {{ border-style:dashed; opacity:0.8 }}
    .card img {{
      width:100%; height:180px; object-fit:contain; background:#0a0c12; border-radius:8px;
      border:1px solid var(--border); transition:border 0.3s;
    }}
    .card img.broken {{ border:2px solid red; }}
    .name {{ font-weight:600; line-height:1.2; }}
    .sub, .meta {{ color:var(--muted); font-size:12px; }}
    .noimg {{ color:#ff6666; font-weight:600; }}
  </style>
</head>
<body>
  <header>
    <form method="get" action="/admin/images">
      <input type="text" name="q" value="{(q or '')}" placeholder="Search brand/name…"/>
      <select name="missing">
        <option value="0" {"selected" if missing == 0 else ""}>With images</option>
        <option value="1" {"selected" if missing == 1 else ""}>Missing images</option>
      </select>
      <select name="sort">
        <option value="newest" {"selected" if sort == "newest" else ""}>Newest first</option>
        <option value="oldest" {"selected" if sort == "oldest" else ""}>Oldest first</option>
      </select>
      <input type="number" name="limit" value="{limit}" min="1" max="1000"/>
      <button type="submit">Apply</button>
    </form>
    <span id="count-info">• Showing {len(rows)} item(s)</span>
  </header>
  <div class="grid">
    {''.join(items) or "<div>No items.</div>"}
  </div>
  <script>
    window.brokenCount = 0;
    window.addEventListener('load', () => {{
      if (window.brokenCount > 0) {{
        document.getElementById('count-info').innerHTML += 
          ` • <span style="color:red">Broken: ${window.brokenCount}</span>`;
      }}
    }});
  </script>
</body>
</html>
    """
    return HTMLResponse(html)
