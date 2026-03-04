import logging
from fastapi import FastAPI
from app.api.webhook import router as webhook_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("app")

app = FastAPI()

app.include_router(webhook_router, prefix="/api")

logger.info("FastAPI app initialized")


@app.get("/")
def root():
    return {"message": "supa-s3-watcher is running"}
