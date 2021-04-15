# YouTube Content Recommendation Service

This repo will host all of the backend code related to fetching Youtube Content in order to showcase on the front-end (comming soon).

The API will handle the RESTful requests sent to the front-end and will communicate the data from Elasticsearch. 

The Content-Engine is responsible for pulling data from YouTube and uploading it properly in Elasticsearch amd maintaining content in Elasticsearch.


## Setup

This project uses Anaconda so access to the `conda` command will be referring to the python virtual environments.
This project also uses `setup.py` to handle packaging, namespacing and installing dependencies.
```bash
conda env create -f environment.yml # create the python environment from the template
conda activate youtube # activates the python environment
python setup.py develop # installs dependencies (to the conda environment)
```

## To run the Content-Engine's scraper

Call the scraper script and supply it with __one__ of the 3 options shown below:

```bash
python aggtube/content-engine/scraper.py popular|categories|top_tags
```
Each option crawls content by that specific criteria from YouTube

## Running the REST API server

```bash
./run_api_server
```
