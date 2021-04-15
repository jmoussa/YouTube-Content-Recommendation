import logging
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
from aggtube.config import config

elasticsearch_mapping = {"mappings": config.mappings}

app = FastAPI()
logger = logging.getLogger(__name__)

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # can alter with time
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    logger.info("Starting up")
    es = Elasticsearch()
    es.indices.create(index=config.index_name, body=elasticsearch_mapping, ignore=400)


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting Down")


# app.include_router(api_router, prefix="/api")
