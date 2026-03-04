from fastapi import FastAPI
from app.api.webhook import router as webhook_router

app = FastAPI()

app.include_router(webhook_router, prefix="/api")


@app.get("/")
def root():
    return {"message": "supa-s3-watcher is running"}
