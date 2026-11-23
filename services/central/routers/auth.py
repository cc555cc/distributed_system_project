"""Demo operator sessions and protected UI delivery; passenger pages stay public."""

import hmac
import os
import secrets
import time
from pathlib import Path
from threading import RLock

from fastapi import APIRouter, Request, Response
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

DEFAULT_USER = "operator"
DEFAULT_PASSWORD = "TransitDemo123!"
COOKIE = "transit_operator"
SESSION_SECONDS = 3600
sessions = {}
session_lock = RLock()
FRONTEND = Path(__file__).resolve().parents[3] / "frontend"
OPERATOR_PAGES = {"control.html", "controls.html", "health.html", "operator.html"}
router = APIRouter(prefix="/auth", tags=["authentication"])


def authenticated(request):
    token = request.cookies.get(COOKIE)
    with session_lock:
        return token is not None and sessions.get(token, 0) > time.monotonic()


class Login(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=256)


@router.post("/login")
def login(body: Login, response: Response):
    valid_user = hmac.compare_digest(body.username.encode(), os.getenv("OPERATOR_USERNAME", DEFAULT_USER).encode())
    valid_password = hmac.compare_digest(body.password.encode(), os.getenv("OPERATOR_PASSWORD", DEFAULT_PASSWORD).encode())
    if not (valid_user & valid_password):
        return JSONResponse({"detail": "Invalid operator credentials"}, status_code=401)
    token = secrets.token_urlsafe(32)
    now = time.monotonic()
    with session_lock:
        for expired in [key for key, expiry in sessions.items() if expiry <= now]:
            del sessions[expired]
        if len(sessions) >= 1000:
            del sessions[next(iter(sessions))]
        sessions[token] = now + SESSION_SECONDS
    response.set_cookie(COOKIE, token, max_age=SESSION_SECONDS, httponly=True,
                        samesite="strict", secure=os.getenv("COOKIE_SECURE", "false").lower() == "true")
    response.headers["Cache-Control"] = "no-store"
    return {"authenticated": True, "role": "operator"}


@router.post("/logout")
def logout(request: Request, response: Response):
    with session_lock:
        sessions.pop(request.cookies.get(COOKIE), None)
    response.delete_cookie(COOKIE)
    return {"authenticated": False}


@router.get("/status")
def status(request: Request):
    return {"authenticated": authenticated(request), "role": "operator" if authenticated(request) else "passenger"}


def install_security(app):
    @app.middleware("http")
    async def operator_boundary(request, call_next):
        path = request.url.path
        page = path.strip("/") in OPERATOR_PAGES
        public = (path in {"/", "/index.html", "/user.html", "/login.html", "/auth/login", "/auth/logout", "/auth/status", "/favicon.ico"}
                  or path.startswith(("/passenger/", "/js/", "/css/")))
        management = not public
        writing = request.method in {"POST", "PUT", "PATCH", "DELETE"}
        auth_route = path in {"/auth/login", "/auth/logout"}
        if writing:
            origin = request.headers.get("origin")
            if origin and origin.rstrip("/") != str(request.base_url).rstrip("/"):
                return JSONResponse({"detail": "Cross-origin management requests are not allowed"}, status_code=403)
        if request.method != "OPTIONS" and (page or management or (writing and not auth_route)) and not authenticated(request):
            if page:
                return RedirectResponse("/login.html?next=" + path, status_code=303)
            return JSONResponse({"detail": "Operator login required"}, status_code=401)
        response = await call_next(request)
        if page or path.startswith("/auth"):
            response.headers["Cache-Control"] = "no-store"
        return response

    app.include_router(router)
    app.mount("/js", StaticFiles(directory=FRONTEND / "js"), name="frontend-js")
    app.mount("/css", StaticFiles(directory=FRONTEND / "css"), name="frontend-css")

    @app.get("/", include_in_schema=False)
    @app.get("/index.html", include_in_schema=False)
    def home():
        return RedirectResponse("/user.html", status_code=303)

    @app.get("/{page}.html", include_in_schema=False)
    def frontend_page(page: str, request: Request):
        filename = page + ".html"
        if filename == "controls.html":
            return RedirectResponse("/control.html", status_code=303)
        if filename not in OPERATOR_PAGES | {"user.html", "login.html"}:
            return JSONResponse({"detail": "Page not found"}, status_code=404)
        return FileResponse(FRONTEND / filename)
