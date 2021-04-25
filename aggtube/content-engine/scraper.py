import argparse
import os
import coloredlogs
import logging
import time
import googleapiclient.discovery
import googleapiclient.errors
import google.auth

from elasticsearch import Elasticsearch, helpers
from aggtube.config import config

elasticsearch_mapping = {"mappings": config.mappings}
elasticsearch_tag_mapping = {"mappings": config.tag_mappings}
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

api_service_name = "youtube"
api_version = "v3"

es = Elasticsearch()
es.indices.create(index=config.content_index, body=elasticsearch_mapping, ignore=400)
es.indices.create(index=config.tags_index, body=elasticsearch_tag_mapping, ignore=400)

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


def crawl_by_keyword(keyword: str, max_scrolls=1):
    items = []
    try:
        request = youtube.search().list(part="snippet", maxResults=50, q=keyword)
        response = request.execute()
        if response["items"]:
            items += response["items"]
            scroll_num = 0
            while response.get("nextPageToken", None) is not None and scroll_num < max_scrolls:
                time.sleep(3)
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


def crawl_popular_content(max_scrolls=1):
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
                time.sleep(3)
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
                    time.sleep(3)
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


def format_for_indexing(content, index, bulk: bool = False):
    doc = {k: v for k, v in content.items() if k != "snippet" and k != "statistics"}

    # content specific code (should probably move out of here)
    if index == config.content_index:
        snippet = content["snippet"]
        doc.update(snippet)
        doc["metrics"] = content["statistics"]
        if doc["metrics"].get("likeCount", None) and doc["metrics"].get("dislikeCount", None):
            doc["metrics"]["likeDislikeRatio"] = float(
                int(doc["metrics"]["likeCount"]) / int(doc["metrics"]["dislikeCount"])
            )

    formatted_bulk_document = {
        "_op_type": "update",
        "_index": index,
        "_id": doc["id"],
        "doc": doc,
        "doc_as_upsert": True,
    }
    return formatted_bulk_document if bulk else doc


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
    logger.info("Indexing Content")
    if crawled_content:
        docs = []
        tag_docs = []
        for c in crawled_content:
            doc = format_for_indexing(c, config.content_index, bulk=True)
            docs.append(doc)

            # tag extraction
            raw_doc = doc["doc"]
            tag_list = raw_doc.get("tags", None)
            if tag_list:
                tag_docs += [
                    format_for_indexing(tag_doc, config.tags_index, bulk=True)
                    for tag_doc in [{"tag": tag, "id": tag} for tag in tag_list]
                ]
            logger.info(f"Processed: {doc['doc']['title']} and {tag_list}")

        # bulk upsert
        helpers.bulk(es, tag_docs + docs)
    else:
        logger.info("No crawled content")
