from fastapi import Request


def get_client_ip(request: Request) -> str:
    """
    Single shared source of truth for client IP extraction, used by both
    RateLimitMiddleware and the per-route @throttle decorator so the two
    protection layers can never disagree about who a request came from.

    Preference order:
      1. X-Real-IP -- if Railway's edge sets this, it's the most direct
         single-value signal.
      2. X-Forwarded-For -- first entry is the original client; preferred
         over request.client.host in case that resolves to Railway's
         internal proxy address rather than the real client.
      3. request.client.host -- last-resort fallback.

    NB: trusting these headers assumes traffic reaches this app only
    through Railway's edge (i.e. Railway strips/overwrites these headers
    rather than passing through whatever a client sends directly). This
    should be confirmed against Railway's actual behavior (debug logging)
    before relying on it as a hard security boundary rather than a
    best-effort rate-limit signal.
    """
    x_real_ip = request.headers.get("x-real-ip")
    if x_real_ip:
        return x_real_ip.strip()

    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()

    return request.client.host if request.client else "unknown"
