import logging
from fastapi import FastAPI
from app.api.webhook import router as webhook_router
from app.api.documents import router as documents_router
from app.core.config import SUPABASE_URL, SUPABASE_SERVICE_KEY
from supabase import acreate_client
from contextlib import asynccontextmanager
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.supabase = await acreate_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    yield

logger = logging.getLogger("app")

app = FastAPI(lifespan=lifespan)

app.include_router(webhook_router, prefix="/api")
app.include_router(documents_router, prefix="/api")

logger.info("FastAPI app initialized")


@app.get("/")
def root():
    return {"message": "supa-s3-watcher is running"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
