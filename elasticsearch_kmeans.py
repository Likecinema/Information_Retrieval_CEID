from pprint import pprint

import elasticsearch as es
import matplotlib.pyplot as plt
import numpy
import pandas as pd
import sklearn
from sklearn import cluster
import sys
client = es.Elasticsearch(host="localhost", port=9200)
#===================================================================================
#elasticsearch search avg per user per category bucket query
#===================================================================================
result = dict(client.search(index='ratings', body={
    "size":0,
  "aggs":{
        "per_user_id":{
          "terms":{
            "field":"userId",
            "size":100000
          },
           "aggs": {
            "unique_vals":{
            "terms": {
            "field":"movie.genres.keyword"
                },
            "aggs":{
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
#===================================================================================
def get_categories(val, uid):
    if uid:
        new_arr = {"userId":uid}
    else:
        new_arr = {}
    for item in val:
        new_arr.update({item["key"]:item["median"]["value"]})
    return new_arr
#===================================================================================
#avg of rating per user per category to dataframe, then nparray (dropping user id column)
#===================================================================================
lista = []
for item in result:
    lista.append( 
        get_categories(item["unique_vals"]["buckets"], uid=item["key"])
    )
df = pd.DataFrame()
for item in lista:
   df =  df.append(item, ignore_index=True)
print(df)
nparr = df.drop(columns='userId').to_numpy(na_value=0)
#===================================================================================
#fit and predict (clustering), getting the centers for every cluster 
#===================================================================================
kmeans = cluster.KMeans().fit(nparr)
y_kmeans = kmeans.predict(nparr)
centers = kmeans.cluster_centers_
cluster_metrics = pd.DataFrame(centers)
#===================================================================================
#creating 2 pandas dataframes: One has genre as column name and row number as cluster, 
#second has userid-cluster number
#===================================================================================
cluster_categories = pd.DataFrame(columns=df.columns.drop('userId'), data=centers)
user_clusters = pd.DataFrame()
user_clusters['userId'] = df['userId']
user_clusters['cluster'] = pd.Series(y_kmeans)
#===================================================================================
print(cluster_categories,"\n", user_clusters)
