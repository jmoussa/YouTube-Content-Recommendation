# Content Engine

This piece is responsible for crawling youtube, fetching content and uplaoding into elasticsearch.

You are required to have a Google service account credentials file to be able to use the YouTube Data API.
Set it in your environment variables:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=path/to/json/credentials/file 
```

More info on authentication and how to obtain [here](https://google-auth.readthedocs.io/en/latest/user-guide.html#service-account-private-key-files)

## Components

### Elasticsearch
An Elasticsearch instance will be in charge of storing the content information and will be the point of contact between the content-engine and the API.

### Scraper
The scraper loads new popular content into Elasticsearch on a schedule (TODO: denote schedule in a crontab).
