import logging
import coloredlogs
from fastapi import APIRouter
from elasticsearch import Elasticsearch
from aggtube.config import config

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)
router = APIRouter()
es = Elasticsearch()


@router.get("/top_liked/{tags}", tags=["Content"])
async def get_top_100_most_liked(tags: str = ""):
    """
    Get Top 100 Most Liked videos

    :param tags: comma separated list of tags to filter, if left as none, then it will not filter by tags
    """
    if tags == "":
        query = {"size": 100, "sort": [{"metrics.viewCount": {"order": "desc"}}]}
    else:
        tags = tags.split(",")
        query = {
            "size": 100,
            "query": {
                "terms": {"tags": tags, "boost": 2.0},
                "range": {"metrics.likeDislikeRatio": {"lte": 1, "boost": 1.0}},
            },
            "sort": [{"metrics.viewCount": {"order": "desc"}}],
        }
    response = es.search(index=config.index_name, body=query)
    try:
        res = response["hits"]["hits"]
        return res
    except Exception as e:
        logger.error(f"Error: ({e}) returning hits, returning entire ES response")
        return response


@router.get("/controversial/{tags}", tags=["Content"])
async def get_videos_with_more_dislikes_than_likes(tags: str = ""):
    """
    Get the top controversial videos using the ratio of likes:dislikes <= 1.
    So a video with more dislikes or a relatively high amount of dislikes will qualify as "controversial"

    :param tags: comma separated list of tags to filter, if left as none, then it will not filter by tags
    """
    if tags == "":
        query = {
            "size": 100,
            "query": {"range": {"metrics.likeDislikeRatio": {"lte": 1, "boost": 2.0}}},
            "sort": [{"metrics.likeDislikeRatio": {"order": "desc"}}],
        }
    else:
        tags = tags.split(",")
        query = {
            "size": 100,
            "query": {
                "terms": {"tags": tags, "boost": 2.0},
                "range": {"metrics.likeDislikeRatio": {"lte": 1, "boost": 1.0}},
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
