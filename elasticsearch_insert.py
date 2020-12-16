import uuid
from datetime import datetime
from multiprocessing.sharedctypes import Array
from pprint import pprint
from uuid import uuid4
import os

import elasticsearch as es
import elasticsearch as es
import pandas as pd
import pandas as pd
from elasticsearch import helpers

start_time = datetime.now()
os.chdir("./Files")
print("\n Indexing start time: ",start_time)
#movies csv to python dictionary
movie_json = pd.read_csv('./movies.csv').to_dict('records')
# "categories" string to array
for entry in movie_json:
    entry['genres'] = entry['genres'].split('|')
#ratings csv to python dictionary
rating_json = pd.read_csv('./ratings.csv').to_dict('records')
#Connecting to Elasticsearch
ES = es.Elasticsearch(host='localhost', port=9200)
ES.indices.create('ratings', ignore=400)

#Input: Python Dictionary, Elasticsearch Index name. Output: Json that can be indexed to elasticsearch
#https://kb.objectrocket.com/elasticsearch/how-to-use-python-helpers-to-bulk-load-data-into-an-elasticsearch-index
def bulk_json_data( json_data, _index, uuid=False):
    for doc in json_data:
        if '{"index"' not in doc:
            if uuid == False:
                yield {
                    "_index": _index,
                    "_id":doc["movieId"],
                    "_source": doc
                }
            else:
                yield {
                    "_index": _index,
                    "_id": uuid4(),
                    "_source": doc
                }
#Creating index, ignore if already exists
ES.indices.create(index='movies', ignore=400,body={
    "settings":{
        "number_of_shards": 2,
        "number_of_replicas":2,
        "index":{
            "similarity":{
                "default":{
                    "type":"BM25"
                }
            }
        }
    },
    "mappings":{
        "standard_mapping":
        {
            "title":{"analyzer":"english"}
            }
        }
    }
)
try:
  movie_response = helpers.bulk(ES,bulk_json_data(movie_json, 'movies', False))
  for index,item in enumerate(rating_json):
    rating_json[index]["movie"] = ES.get(index='movies', id=item["movieId"])["_source"]
  rating_response = helpers.bulk(ES,bulk_json_data(rating_json, 'ratings', True))
except Exception as e:
  print("\nError:", e)

for index,item in enumerate(movie_json):
  movie_index = index
  #movie_json[index]["ratings"] = ES.search(index='ratings', body={
  res = ES.search(index='ratings', body={
      "query":{
        "match":{
        "movieId":item["movieId"]}}})['hits']['hits']
  rating_list = []
  for index,item in enumerate(res):
        rating_list.append({
          'rating':item['_source']['rating'],
          'userId':item['_source']['userId']}
          )
  movie_json[movie_index]['ratings'] = rating_list

try: 
#bulk insert movie json data to elasticsearch
  response = helpers.bulk(ES, bulk_json_data(movie_json, 'movies'))
except Exception as e:
  print("\nError:", e)
#pprint(movie_json)
end_time = datetime.now()
print("\nIndexing end time: ",end_time)
print("\nTime indexing took: ", end_time-start_time)



"""
We shall keep that
GET /ratings/_search?size=0
{
  "aggs":{
    "unique_vals":{
      "terms": {
        "field":"movie.genres.keyword"
      },
      "aggs":{
        "per_user_id":{
          "terms":{
            "field":"userId"
          }
          , "aggs": {
            "median": {
              "avg": {
                "field": "rating"
              }
            }
          }
        }
      }
    }
  }
}"""
