import logging
import coloredlogs
from fastapi import APIRouter
from elasticsearch import Elasticsearch
from aggtube.config import config

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)
router = APIRouter()
es = Elasticsearch()


@router.get("/most_liked", tags=["Popular Content"])
async def get_top_100_most_liked():
    """
    Get Top 100 Most Liked videos
    TODO: Use better metric for popularity?
    """
    query = {"size": 100, "sort": [{"statistics.viewCount": {"order": "desc"}}]}
    response = es.search(index=config.index_name, body=query)
    try:
        res = response["hits"]["hits"]
        return res
    except Exception as e:
        logger.error(f"Error: ({e}) returning hits, returning entire ES response")
        return response


@router.get("/controversial", tags=["Popular Content"])
async def get_top_100_controversial():
    """
    Most controversial is grabbed using this function: dislikes > likes
    """
    query = {
        "size": 100,
        "script_fields": {
            "controversy_diff": {"script": 'doc["statistics.likeCount"].value - doc["statistics.dislikeCount"].value'}
        },
    }
    response = es.search(index=config.index_name, body=query)
    try:
        res = response["hits"]["hits"]
        logger.info(res)
        return res
    except Exception as e:
        logger.error(f"Error: ({e}) returning hits, returning entire ES response")
        return response
