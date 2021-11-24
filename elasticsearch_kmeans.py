import elasticsearch as es
import numpy as np
import pandas as pd
import sklearn
import os
from sklearn import cluster
from elasticsearch import helpers
from itertools import product
import sys

os.chdir("./Files")
client = es.Elasticsearch(host="localhost", port=9200)
# ===================================================================================
# elasticsearch search avg per user per category bucket query
# ===================================================================================
result = dict(client.search(index='ratings', body={
    "size": 0,
    "aggs": {
        "per_user_id": {
            "terms": {
              "field": "userId",
              "size": 1000000
              },
            "aggs": {
                "unique_vals": {
                    "terms": {
                        "field": "genres.keyword"
                    },
                    "aggs": {
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
}))["aggregations"]["per_user_id"]["buckets"]

# ===================================================================================


def get_categories(val, uid):
    if uid:
        new_arr = {"userId": uid}
    else:
        new_arr = {}
    for item in val:
        new_arr.update({item["key"]: item["median"]["value"]})
    return new_arr


# ===================================================================================
# avg of rating per user per category to dataframe, then nparray (dropping user id column)
# ===================================================================================
lista = []
for item in result:
    lista.append(
        get_categories(item["unique_vals"]["buckets"], uid=item["key"])
    )
df = pd.DataFrame()
for item in lista:
    df = df.append(item, ignore_index=True)
nparr = df.drop(columns='userId').to_numpy(na_value=0)
# ===================================================================================
# fit and predict (clustering), getting the centers for every cluster
# ===================================================================================
kmeans = cluster.KMeans().fit(nparr)
y_kmeans = kmeans.predict(nparr)

# # ===================================================================================
# creating pandas dataframe that has userid->cluster
# ===================================================================================
user_clusters = pd.DataFrame()
user_clusters['userId'] = df['userId']
user_clusters['cluster'] = pd.Series(y_kmeans)

movies = client.search(index="movies", body={
    "size": 10000,
    "query": {
        "match_all": {}
    }
})["hits"]["hits"]

movies_arr = []
for item in movies:
    movies_arr.append(item["_source"])
df_movies_arr = pd.DataFrame(movies_arr)
# ===================================================================================
gb_user_clusters = user_clusters.groupby('cluster')
for key, item in gb_user_clusters:
    print("CLUSTER", key)
    print(gb_user_clusters.get_group(key)['userId'].to_list(), "\n")

# ===================================================================================
# elasticsearch search avg per user per movie in cluster
# ===================================================================================
    x = pd.DataFrame(dict(client.search(index='ratings', body={
        "size": 0,
        "query": {
            "terms": {
                "userId":  gb_user_clusters.get_group(key)['userId'].to_list()
            }
        },
        "aggs": {
            "movies": {
                "terms": {
                    "size": 1000,
                    "field": "movieId"
                },
                "aggs": {
                    "median": {
                        "avg": {
                            "field": "rating"
                        }
                    }
                }
            }
        }
    }))["aggregations"]["movies"]["buckets"])
    x["median"] = x["median"].apply(lambda x: x['value'])
    x = x.drop(columns="doc_count")

    # ===================================================================================
    # dataframe to join users with movie ratings
    # ===================================================================================
    movie_user_df = pd.DataFrame(
        list(product(x["key"], gb_user_clusters.get_group(key)['userId'].to_list())), columns=["movieId", "userId"])
    movie_user_df = pd.merge(df_movies_arr, movie_user_df, on="movieId")
    movie_user_df = pd.merge(
        left=movie_user_df, right=x, how="left",  left_on=movie_user_df["movieId"], right_on=x["key"])
    movie_user_df = movie_user_df.drop(columns=["key_0", "key"])
    # ===================================================================================
    # elasticsearch create new index kmeans ratings and update with new values
    # ===================================================================================
    dict_movie_user_df = movie_user_df.to_dict(orient='records')

    index_update_json = [
        {"_index": "ratings",
         "_op_type": "update",
         "_id": str(float(item["userId"])) + str(int(item["movieId"])),
            "_source": {"doc":
                        item}

         }
        for item in dict_movie_user_df
    ]
    helpers.bulk(client, index_update_json, raise_on_exception=False,
                 raise_on_error=False)

    index_create_json = [
        {"_index": "ratings",
         "_op_type": "create",
         "_id": str(float(item["userId"])) + str(int(item["movieId"])),
         "_source": item

         }
        for item in dict_movie_user_df
    ]
    helpers.bulk(client, index_create_json, raise_on_exception=False,
                 raise_on_error=False)
