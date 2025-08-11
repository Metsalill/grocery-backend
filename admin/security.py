# admin/security.py
import base64
from fastapi import HTTPException, Request, status
from settings import ADMIN_IP_ALLOWLIST, SWAGGER_USERNAME, SWAGGER_PASSWORD

def _admin_ip_allowed(req: Request) -> bool:
    if not ADMIN_IP_ALLOWLIST:
        return True
    allowed = {ip.strip() for ip in ADMIN_IP_ALLOWLIST.split(",") if ip.strip()}
    return req.client and req.client.host in allowed

def basic_guard(req: Request):
    if not _admin_ip_allowed(req):
        raise HTTPException(status_code=403, detail="Admin IP not allowed")
    if not (SWAGGER_USERNAME and SWAGGER_PASSWORD):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Admin auth not configured")
    expected = "Basic " + base64.b64encode(f"{SWAGGER_USERNAME}:{SWAGGER_PASSWORD}".encode()).decode()
    auth = req.headers.get("Authorization")
    if auth != expected:
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})
