# middlewares/docs_guard.py
import base64
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

class SwaggerAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, username: str | None, password: str | None):
        super().__init__(app)
        self.username = username
        self.password = password

    async def dispatch(self, request: Request, call_next):
        protected = request.url.path in ("/docs", "/redoc", "/openapi.json")
        if protected:
            if not (self.username and self.password):
                return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic"})
            auth = request.headers.get("Authorization")
            expected = "Basic " + base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            if auth != expected:
                return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic"})
        return await call_next(request)
