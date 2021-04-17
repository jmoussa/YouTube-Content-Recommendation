import argparse
import os
import coloredlogs
import logging

import googleapiclient.discovery
import googleapiclient.errors
import google.auth

from elasticsearch import Elasticsearch
from aggtube.config import config

elasticsearch_mapping = {"mappings": config.mappings}
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

api_service_name = "youtube"
api_version = "v3"

es = Elasticsearch()
es.indices.create(index=config.index_name, body=elasticsearch_mapping, ignore=400)

# Get credentials and create an API client
credentials, project = google.auth.default(scopes=scopes)
youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=credentials)


def get_top_tags_and_crawl():
    query = {"size": 0, "aggs": {"tags": {"terms": {"field": "tags", "size": 50}}}}
    response = es.search(index=config.index_name, body=query)
    items = []
    # Get tag names and crawl (youtube.search) youtube and add them to `items`
    for bucket in response["aggregations"]["tags"]["buckets"]:
        logger.info(f"Querying {bucket['key']}")
        content = crawl_by_keyword(bucket["key"])
        items += content
    logger.info(f"Parsed {len(items)} pieces of content")
    return items


def crawl_by_keyword(keyword: str, max_scrolls=10):
    items = []
    try:
        request = youtube.search().list(part="snippet", maxResults=50, q=keyword)
        response = request.execute()
        if response["items"]:
            items += response["items"]
            scroll_num = 0
            while response.get("nextPageToken", None) is not None and scroll_num < max_scrolls:
                request = youtube.search().list(part="snippet", maxResults=50, q=keyword)
                response = request.execute()
                scroll_num += 1
                if response["items"]:
                    items += response["items"]
                logger.info(f"Pass #{scroll_num}: Count {len(items)}")
            logger.info(f"Completed {scroll_num} scrolls")
            return items
        else:
            logger.error("No content found")
            return items
    except Exception as e:
        logger.error(e)
        return items


def crawl_popular_content(max_scrolls=10):
    items = []
    try:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics", maxResults=1000, chart="mostPopular", regionCode="US"
        )
        response = request.execute()
        if response["items"]:
            items += response["items"]
            scroll_num = 0
            while response.get("nextPageToken", None) is not None and scroll_num < max_scrolls:
                request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    maxResults=1000,
                    chart="mostPopular",
                    regionCode="US",
                    pageToken=response["nextPageToken"],
                )
                response = request.execute()
                scroll_num += 1
                if response["items"]:
                    items += response["items"]
                logger.info(f"Pass #{scroll_num}: Count {len(items)}")
            logger.info(f"Completed {scroll_num} scrolls")
            return items
        else:
            logger.error("No content found")
            return items
    except Exception as e:
        logger.error(e)
        return items


def get_categories():
    """
    Returns a dictionary of category title to category id
    """
    try:
        request = youtube.videoCategories().list(part="snippet", regionCode="US")
        response = request.execute()
        return {i["snippet"]["title"]: i["id"] for i in response["items"]}
    except Exception as e:
        logger.error(f"Exception: {e}")
        raise Exception(e)


def crawl_category(category_name: str):
    category_name_to_id = get_categories()
    if category_name_to_id.get(category_name, None):
        items = []
        try:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                maxResults=1000,
                chart="mostPopular",
                regionCode="US",
                videoCategoryId="1",
            )
            response = request.execute()
            if response["items"]:
                items += response["items"]
                scroll_num = 0
                while response.get("nextPageToken", None) is not None:
                    request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        maxResults=1000,
                        chart="mostPopular",
                        regionCode="US",
                        videoCategoryId="1",
                        pageToken=response["nextPageToken"],
                    )
                    response = request.execute()
                    scroll_num += 1
                    if response["items"]:
                        items += response["items"]
                    logger.info(f"Pass #{scroll_num}: Count {len(items)}")
                logger.info(f"Completed {scroll_num} scrolls")
                return items
            else:
                logger.error("No content found")
                return items
        except Exception as e:
            logger.error(e)
            return items
    else:
        logger.error(f"Category name not valid: {category_name}")
        raise Exception(f"Not a valid category: {category_name}")


def format_for_indexing(content):
    doc = {k: v for k, v in content.items() if k != "snippet" and k != "statistics"}
    snippet = c["snippet"]
    doc.update(snippet)
    doc["metrics"] = content["statistics"]
    doc["like_dislike_ratio"] = content["statistics"]["likeCount"] / content["statistics"]["dislikeCount"]
    return doc


if __name__ == "__main__":
    # logger.info(f"--------------------CATEGORIES--------------------\n{json.dumps(get_categories(), indent=2)}")
    parser = argparse.ArgumentParser()
    parser.add_argument("type", default="popular", help="Type of YouTube content (popular, categories, top_tags)")
    args = parser.parse_args()

    if args.type == "popular":
        logger.warning("Crawling Popular Content")
        crawled_content = crawl_popular_content()
    elif args.type == "categories":
        crawled_content = []
        for cat_name, cat_id in get_categories().items():
            logger.info(f"Crawling {cat_name} content")
            crawled_content += crawl_category(cat_name)
    elif args.type == "top_tags":
        crawled_content = get_top_tags_and_crawl()
    else:
        raise Exception("Wrong args supplied")

    logger.info("Finished Crawling")
    if crawled_content:
        for c in crawled_content:
            doc = format_for_indexing(c)

            es.update(index=config.index_name, id=c["id"], body={"doc": doc, "doc_as_upsert": True})
            logger.info(f"Crawled: {c['snippet']['title']}")
    else:
        logger.info("No crawled content")
