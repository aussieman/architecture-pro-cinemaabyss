import os
import random
import logging
from fastapi import FastAPI, Request, Response
import httpx

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI()

# --- Конфигурация ---
PORT = int(os.getenv("PORT", 8000))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")

GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))


# --- Универсальная прокси-функция ---
async def proxy_request(request: Request, target_url: str, target_name: str) -> Response:
    async with httpx.AsyncClient() as client:
        url = f"{target_url}{request.url.path}"
        logger.info(f"➡️  Proxying {request.method} {request.url.path} → {target_name}")
        resp = await client.request(
            method=request.method,
            url=url,
            params=request.query_params,
            headers=request.headers,
            content=await request.body(),
        )
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=dict(resp.headers),
        )


# --- Маршруты ---
@app.api_route("/api/movies{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def movies_proxy(request: Request, path: str = ""):
    if GRADUAL_MIGRATION:
        if random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
            return await proxy_request(request, MOVIES_SERVICE_URL, "movies-service")
    return await proxy_request(request, MONOLITH_URL, "monolith")


@app.api_route("/api/events{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def events_proxy(request: Request, path: str = ""):
    return await proxy_request(request, EVENTS_SERVICE_URL, "events-service")


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def monolith_proxy(request: Request, path: str = ""):
    return await proxy_request(request, MONOLITH_URL, "monolith")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        reload=False,
        log_level="info"
    )
