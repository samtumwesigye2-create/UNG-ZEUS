import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from db import Base, engine
from catalog_db import ObjectVersion  # noqa: F401
from zeus import router as zeus_router

Base.metadata.create_all(bind=engine)
app = FastAPI(title="UNG-ZEUS", description="Uganda National Grid National Data Storage Platform")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("ALLOWED_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(zeus_router)


@app.get("/")
def root():
    return {"service": "UNG-ZEUS", "status": "ok", "role": "National Data Storage Platform"}


@app.get("/health")
def health():
    return {"service": "UNG-ZEUS", "status": "ok"}
