import gensim
from lenskit.algorithms import tf
from elasticsearch import helpers
import pandas as pd
import os
import lenskit as lk
import numpy as np
import elasticsearch as es

os.chdir("./Files")


def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):

    percent = ("{0:." + str(decimals) + "f}").format(100 *
                                                     (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


movies = pd.read_csv("./movies.csv")
movies["genres"] = movies["genres"].apply(lambda x: x.split("|"))
one_hot = pd.get_dummies(movies.genres.apply(
    pd.Series).stack()).sum(level=0).to_numpy()

ratings = pd.read_csv("./ratings.csv")
movie_titles = pd.DataFrame(movies["title"].apply(lambda x: x.split(" ")))

output_df = pd.merge(movies, ratings, on="movieId")

# train model
model = gensim.models.Word2Vec(
    sentences=movie_titles["title"], size=10, min_count=1)
print(model)
# summarize vocabulary
words = list(model.wv.vocab)
# save model
model.save('model.bin')
# load model
new_model = gensim.models.Word2Vec.load('model.bin')

movie_titles["vector"] = np.nan
movie_titles["vector"] = movie_titles["vector"].astype('object')
for index, item in enumerate(movie_titles["title"]):
    vector = np.mean(np.array([new_model[x] for x in item]), axis=0)
    vector = np.concatenate([vector, one_hot[index]])
    movie_titles.at[index, 'vector'] = np.ndarray.tolist(vector)

movie_titles["title"] = movie_titles["title"].apply(lambda x: " ".join(x))
movie_titles = pd.merge(movie_titles, output_df, on="title")
movie_titles = movie_titles.drop(
    columns=["genres"])
movie_titles.rename(columns={"vector": "item", "userId": "user"}, inplace=True)
movie_titles["item"] = movie_titles["item"].apply(lambda x: str(x))
print(movie_titles.head())

net = tf.BPR(features=30, epochs=80)
net = net.fit(movie_titles.reset_index(drop=True))

un_user = output_df.userId.unique()

result = pd.Series()
full_result = pd.DataFrame()


i = 0
printProgressBar(0, len(un_user), prefix='Progress:',
                 suffix='Complete', length=50)
for user in un_user:
    result = pd.DataFrame(net.predict_for_user(
        int(user), [x for x in movie_titles.item.unique()])).reset_index()
    result["user"] = int(user)
    full_result = full_result.append(result, ignore_index=True)
    i = i+1
    printProgressBar(i + 1, len(un_user), prefix='Progress:',
                     suffix='Complete', length=50)

full_result = pd.DataFrame(full_result).reset_index()
full_result.columns = ['index', 'item', 'predicted_rating', 'userId']
movie_titles = movie_titles.drop(
    columns=["user", "rating", "timestamp"]).drop_duplicates()
full_result = pd.merge(full_result, movie_titles, on="item")
full_result = full_result.sort_values(
    ['predicted_rating'], ascending=[False]).drop_duplicates()
full_result = full_result.drop(columns=["index", "item"])
full_result.predicted_rating = full_result.predicted_rating.mask(
    full_result.predicted_rating.lt(0), 0)

print(full_result.head())

full_result = full_result.to_dict(orient="records")

print(len(full_result))

client = es.Elasticsearch(host="localhost", port=9200)
index_update_pred = [
    {"_index": "ratings",
     "_op_type": "update",
     "_id": str(float(item["userId"])) + str(int(item["movieId"])),
     "_source": {"doc":
                 item}

     }
    for item in full_result
]

helpers.bulk(client, index_update_pred, raise_on_exception=False,
             raise_on_error=False)

index_create_json = [
    {"_index": "ratings",
     "_op_type": "create",
     "_id": str(float(item["userId"])) + str(int(item["movieId"])),
     "_source":
     item

     }
    for item in full_result
]

helpers.bulk(client, index_create_json, raise_on_exception=False,
             raise_on_error=False)
