import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch()

es.indices.delete(index='states', ignore=[400, 404])
es.indices.delete(index='contacts', ignore=[400, 404])


doc = {
    'author': 'deep blue',
    'text': 'Geo data 2 Elasticsearch',
    'timestamp': datetime.datetime.now(),
}

es.index(index="states", doc_type='geo_data', id=1, body=doc)
es.index(index="contacts", doc_type='geo_data', id=1, body=doc)