import elasticsearch as es
import sys
from pprint import pprint
client = es.Elasticsearch(host="localhost", port=9200)
def search():
    print("Search For movie, type exit() to exit")
    term = input()
    if term.lower() == 'exit()':
        sys.exit()
    else:
        res = dict(client.search(index='movies', body={
            "query":{
                "fuzzy":{
                    "title": term.lower()
                }
            }
        }))
        for entry in res["hits"]["hits"]:
            print(entry["_source"]["title"], "\t", entry['_score'])
while True:
    search()