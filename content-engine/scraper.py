import argparse
import os

# import json
import coloredlogs
import logging

import googleapiclient.discovery
import googleapiclient.errors
import google.auth

from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

api_service_name = "youtube"
api_version = "v3"

# Get credentials and create an API client

credentials, project = google.auth.default(scopes=scopes)
youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=credentials)


def crawl_popular_content():
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics", maxResults=1000, chart="mostPopular", regionCode="US"
    )
    response = request.execute()
    logger.info([i["id"] + "\n" for i in response["items"]])
    return response["items"]


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
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            maxResults=1000,
            chart="mostPopular",
            regionCode="US",
            videoCategoryId="1",
        )
        response = request.execute()
        return response["items"]
    else:
        logger.error(f"Category name not valid: {category_name}")
        raise Exception(f"Not a valid category: {category_name}")


if __name__ == "__main__":
    # logger.info(f"--------------------CATEGORIES--------------------\n{json.dumps(get_categories(), indent=2)}")
    es = Elasticsearch()
    es.indices.create(index="youtube", ignore=400)

    parser = argparse.ArgumentParser()
    parser.add_argument("type", default="by_category", help="Type of YouTube content (popular, by_category)")
    parser.add_argument(
        "--category",
    )
    args = parser.parse_args()

    if args.type == "popular":
        logger.warning("Crawling Popular Content")
        crawled_content = crawl_popular_content()
    elif args.type == "by_category":
        if args.category:
            logger.info("Crawling {args.category} content")
            crawled_content = crawl_category(args.category)
        else:
            logger.error("No category anme specified")
            raise Exception("Please supply `--category <category_name>` arguments")
    else:
        raise Exception("Wrong args supplied")

    logger.info("Finished Crawling")
    if crawled_content:
        for c in crawled_content:
            es.update(index="youtube", id=c["id"], body={"doc": c, "doc_as_upsert": True})
            logger.info(f"Crawled: {c['snippet']['title']}")
