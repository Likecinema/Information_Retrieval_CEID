import elasticsearch as es
import sys
client = es.Elasticsearch(host="localhost", port=9200)


def search():
    print(
        "Search For movie, type exit() to exit, type [movie name] -u [id] to search for movie based on id")
    term = input()
    if term.lower() == 'exit()':
        sys.exit()
    elif "-u" in term.lower():
        uid = int(term.split(" -u ")[1])
        movie = term.split(" -u ")[0]
        res = dict(
            client.search(index='ratings', body={
                "query": {
                    "function_score": {
                        "query": {
                                "match": {
                                    "title": {
                                        "query": movie,
                                        "fuzziness": "AUTO"
                                    }
                                }
                        },
                        "boost": "5",
                        "functions": [
                            {
                                "weight": 50,
                                "filter": {
                                    "term": {
                                        "userId": uid
                                    }
                                }
                            },
                            {
                                "field_value_factor": {
                                    "field": "rating",
                                    "factor": 30,
                                    "modifier": "none",
                                                "missing": 1
                                }
                            },
                            {
                                "field_value_factor": {
                                    "field": "median",
                                    "factor": 20,
                                    "modifier": "none",
                                                "missing": 1
                                }
                            },
                            {
                                "field_value_factor": {
                                    "field": "predicted_rating",
                                    "factor": 25,
                                    "modifier": "none",
                                                "missing": 1
                                }
                            }
                        ],
                        "score_mode": "avg",
                        "boost_mode": "multiply"
                    }
                },
                "size": 0,
                "aggs": {
                    "variant_groups": {
                        "terms": {
                            "field": "title.keyword",
                            "size": 20,
                            "missing": "No group",
                            "order": {
                                "max_score": "desc"
                            }
                        },
                        "aggs": {
                            "max_score": {
                                "avg": {
                                    "script": "_score"
                                }
                            }
                        }
                    }
                }
            }))
        for item in res["aggregations"]["variant_groups"]["buckets"]:
            print(item["key"])
    else:
        res = dict(client.search(index='movies', body={
            "query": {
                "fuzzy": {
                    "title": term.lower()
                }
            }
        }))
        for entry in res["hits"]["hits"]:
            print(entry["_source"]["title"], "\t", entry['_score'])


while True:
    search()
