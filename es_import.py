# ["date/time", "yymmdd", "hummus.SSS", "platform", "colour", "position", "course", "speed", "depth"]

from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch()
error_log_file = open("error_log.txt", "w+")


def prepare_data(states, contacts):

    for entry in states:
        del entry["es_index"]
        yield {
            '_op_type': 'index',
            "_index": "states",
            "_type": "geo_data",
            "_source": entry
        }

    for entry in contacts:
        del entry["es_index"]
        yield {
            '_op_type': 'index',
            "_index": "contacts",
            "_type": "geo_data",
            "_source": entry
        }


def store_data(states, contacts):
    print("Transferring the data")
    helpers.bulk(es, prepare_data(states, contacts), chunk_size=1000)
