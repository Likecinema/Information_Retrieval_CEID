import os
import elasticsearch as es
import numpy as np
import pandas as pd
from elasticsearch import helpers

os.chdir("./Files")

ES = es.Elasticsearch(host="localhost", port=9200)
ES.indices.create(index="ratings", ignore=400)
ES.indices.create(index="movies", ignore=400)
df_movies = pd.read_csv("./movies.csv")
df_movies["genres"] = df_movies["genres"].apply(lambda x: x.split("|"))

df_ratings = pd.read_csv("./ratings.csv")
df_2 = pd.merge(df_movies, df_ratings, on='movieId', how='outer')
df_2 = df_2.replace(np.nan, None, regex=True)

jeyson = df_movies.to_dict(orient='records')
index_json = [
    {
        "_index": "movies",
        "_source": item
    }
    for item in jeyson
]
helpers.bulk(ES, index_json)

jason = df_2.to_dict(orient='records')
index_json = [
    {"_index": "ratings",
     "_source": item,
     "_id": (str(float(item["userId"])) + str(int(item["movieId"])))
     }
    for item in jason
]
helpers.bulk(ES, index_json)
