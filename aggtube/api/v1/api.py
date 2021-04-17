import logging
import coloredlogs
from fastapi import APIRouter
from elasticsearch import Elasticsearch
from aggtube.config import config

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)
router = APIRouter()
es = Elasticsearch()


@router.get("/liked", tags=["Content"])
async def get_top_100_most_liked():
    """
    Get Top 100 Most Liked videos
    TODO: Use better metric for popularity?
    """
    query = {"size": 100, "sort": [{"metrics.viewCount": {"order": "desc"}}]}
    response = es.search(index=config.index_name, body=query)
    try:
        res = response["hits"]["hits"]
        return res
    except Exception as e:
        logger.error(f"Error: ({e}) returning hits, returning entire ES response")
        return response


@router.get("/controversial", tags=["Content"])
async def get_videos_with_more_dislikes_than_likes():
    """
    Most controversial is grabbed using this function: dislikes > likes
    """
    query = {
        "size": 100,
        "query": {
            "range": {"metrics.likeDislikeRatio": {"lte": 1, "boost": 2.0}},
        },
        "sort": [{"metrics.likeDislikeRatio": {"order": "desc"}}],
    }
    response = es.search(index=config.index_name, body=query)
    try:
        res = response["hits"]["hits"]
        logger.info(res)
        return res
    except Exception as e:
        logger.error(f"Error: ({e}) returning hits, returning entire ES response")
        return response
