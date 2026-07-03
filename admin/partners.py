# admin/partners.py
#
# Simple internal admin UI for managing analytics_partners rows — the
# people/brands who get a token for /admin/analytics (in addition to the
# existing 5 chain tokens, which remain in Railway env vars unchanged).
#
# Protected by the same basic_guard as the main "/" admin dashboard.
# Not partner-facing — this is Marko's own tool for creating a partner,
# copying their token, and sending it to them.

import os
import secrets
from html import escape
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from .security import basic_guard

router = APIRouter()


def _generate_token() -> str:
    # 32 bytes -> 43-char urlsafe token. Same order of magnitude as the
    # chain tokens already in use, generated the same way you'd generate
    # any of the ANALYTICS_TOKEN_* env var values.
    return secrets.token_urlsafe(32)


def _parse_brand_list(raw: str) -> list[str]:
    # Comma-separated input -> cleaned list, e.g. "Farmi, FARMI" -> ['Farmi', 'FARMI']
    if not raw:
        return []
    return [b.strip() for b in raw.split(",") if b.strip()]


@router.get("/admin/partners", response_class=HTMLResponse, dependencies=[Depends(basic_guard)])
async def list_partners(request: Request):
    if getattr(request.app.state, "db", None) is None:
        return HTMLResponse("<h2>DB not ready yet. Try again in a few seconds.</h2>", status_code=503)

    async with request.app.state.db.acquire() as conn:
        partners = await conn.fetch("""
            SELECT id, partner_type, name, token, brand_filter, chain_filter, created_at
            FROM analytics_partners
            ORDER BY created_at DESC
        """)

    _base_url = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")

    rows_html = ""
    for p in partners:
        brand_list = ", ".join(p["brand_filter"] or [])
        link = f"{_base_url}/admin/analytics?token={p['token']}" if _base_url else f"/admin/analytics?token={p['token']}"
        rows_html += f"""
        <tr>
            <td>{p['id']}</td>
            <td><b>{escape(p['name'])}</b></td>
            <td>{escape(p['partner_type'])}</td>
            <td style="font-size:0.75rem;color:#666">{escape(brand_list) or '—'}</td>
            <td style="font-size:0.75rem;color:#666">{escape(p['chain_filter'] or '') or '—'}</td>
            <td>
                <div style="display:flex;gap:6px;align-items:center">
                    <code style="font-size:0.72rem;background:#f0f0f0;padding:3px 6px;border-radius:4px;word-break:break-all">{escape(p['token'])}</code>
                </div>
                <a href="{escape(link)}" target="_blank" style="font-size:0.75rem;color:#FF9100">Ava dashboard →</a>
            </td>
            <td style="font-size:0.75rem;color:#888">{p['created_at'].strftime('%d.%m.%Y')}</td>
            <td>
                <form method="post" action="/admin/partners/{p['id']}/delete" onsubmit="return confirm('Kustutada partner {escape(p['name'])}? Tema token lakkab kohe töötamast.');">
                    <button type="submit" style="background:#e74c3c;color:white;border:none;padding:6px 10px;border-radius:6px;font-size:0.75rem;cursor:pointer">Kustuta</button>
                </form>
            </td>
        </tr>"""

    if not rows_html:
        rows_html = '<tr><td colspan="8" style="text-align:center;color:#888;padding:24px">Partnereid pole veel lisatud.</td></tr>'

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Seivy Admin — Partnerid</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #f5f5f5; margin: 0; padding: 16px; color: #222; }}
  h1 {{ color: #C94B7C; margin-bottom: 4px; }}
  a.back {{ color: #888; font-size: 0.85rem; text-decoration: none; }}
  .card {{ background: white; border-radius: 10px; padding: 20px; margin-top: 16px;
           box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
  th {{ background: #f0f0f0; padding: 8px 10px; text-align: left; font-size: 0.78rem; color: #555; }}
  td {{ padding: 8px 10px; border-top: 1px solid #f0f0f0; font-size: 0.85rem; vertical-align: top; }}
  label {{ display: block; font-size: 0.82rem; font-weight: 600; color: #444; margin: 12px 0 4px; }}
  input[type=text], select {{ width: 100%; padding: 8px 10px; border: 1px solid #ddd;
    border-radius: 8px; font-size: 0.9rem; box-sizing: border-box; }}
  .hint {{ font-size: 0.75rem; color: #888; margin-top: 3px; }}
  .submit-btn {{ margin-top: 16px; background: #FF9100; color: white; border: none;
    padding: 10px 20px; border-radius: 8px; font-weight: 600; cursor: pointer; font-size: 0.9rem; }}
</style>
</head><body>
<a class="back" href="/">← Tagasi Admin Dashboardile</a>
<h1>Analüütika partnerid</h1>
<p style="color:#666;font-size:0.9rem">Ketid kasutavad endiselt Railway env-muutujaid (ANALYTICS_TOKEN_*). Siin lisatud partnerid (peamiselt tootjad/brändid) saavad oma tokeniga ligipääsu partnerdashboardile.</p>

<div class="card">
  <h2 style="font-size:1rem;margin-top:0">Olemasolevad partnerid</h2>
  <table>
    <tr><th>ID</th><th>Nimi</th><th>Tüüp</th><th>Bränd(id)</th><th>Kett</th><th>Token</th><th>Lisatud</th><th></th></tr>
    {rows_html}
  </table>
</div>

<div class="card">
  <h2 style="font-size:1rem;margin-top:0">Lisa uus partner</h2>
  <form method="post" action="/admin/partners/create">
    <label>Nimi (kuvatakse dashboard'il)</label>
    <input type="text" name="name" placeholder="nt Farmi" required>

    <label>Tüüp</label>
    <select name="partner_type" required>
      <option value="brand">Tootja / bränd</option>
      <option value="retailer">Jaekett (harva vajalik — ketid on juba env-muutujates)</option>
    </select>

    <label>Brändi nimed (ainult tootja tüübile)</label>
    <input type="text" name="brand_names" placeholder="nt Farmi, FARMI">
    <div class="hint">Komadega eraldatud, kõik teadaolevad kirjapildi variandid product_groups.brand väljast. Täpne vaste (case-insensitive), et vältida valesid tulemusi.</div>

    <label>Ketifilter (ainult jaeketi tüübile, valikuline)</label>
    <input type="text" name="chain_filter" placeholder="nt selver">
    <div class="hint">Väiketähtedega, vastab stores.chain / products.chain väärtusele.</div>

    <button type="submit" class="submit-btn">Loo partner ja genereeri token</button>
  </form>
</div>

</body></html>"""
    return HTMLResponse(html)


@router.post("/admin/partners/create", dependencies=[Depends(basic_guard)])
async def create_partner(
    request: Request,
    name: str = Form(...),
    partner_type: str = Form(...),
    brand_names: str = Form(""),
    chain_filter: str = Form(""),
):
    if partner_type not in ("brand", "retailer"):
        raise HTTPException(status_code=400, detail="Invalid partner_type")

    name = name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")

    brand_list = _parse_brand_list(brand_names) if partner_type == "brand" else []
    chain_value = chain_filter.strip().lower() if partner_type == "retailer" and chain_filter.strip() else None

    if partner_type == "brand" and not brand_list:
        raise HTTPException(status_code=400, detail="At least one brand name is required for brand partners")

    token = _generate_token()

    if getattr(request.app.state, "db", None) is None:
        raise HTTPException(status_code=503, detail="Database not ready")

    async with request.app.state.db.acquire() as conn:
        await conn.execute("""
            INSERT INTO analytics_partners (partner_type, name, token, brand_filter, chain_filter)
            VALUES ($1, $2, $3, $4, $5)
        """, partner_type, name, token, brand_list or None, chain_value)

    return RedirectResponse(url="/admin/partners", status_code=303)


@router.post("/admin/partners/{partner_id}/delete", dependencies=[Depends(basic_guard)])
async def delete_partner(request: Request, partner_id: int):
    if getattr(request.app.state, "db", None) is None:
        raise HTTPException(status_code=503, detail="Database not ready")

    async with request.app.state.db.acquire() as conn:
        await conn.execute("DELETE FROM analytics_partners WHERE id = $1", partner_id)

    return RedirectResponse(url="/admin/partners", status_code=303)
