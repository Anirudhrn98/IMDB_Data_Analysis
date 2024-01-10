"""
Subject: INTRODUCTION TO BIG DATA
Assingment : 7-Q2
Author: Anirudh Narayanan
"""
import pymongo


def create_centroids(g, k):
    # Create Connection
    client = pymongo.MongoClient()
    db = client['Clustering']

    # Drop collection centroids if it exists
    if 'centroids' in db.list_collection_names():
        db.centroids.drop()

    # sample 3 documents after filtering out movies which the field kmeansNorm and matching genre
    query = [{'$match': {'$and': [{'kmeansNorm': {'$exists': True}},
                                  {'genres': {"$in": [g]}}]}},
             {'$sample': {'size': k}}]

    result = db.movies.aggregate(query)
    # Create initial centroids from random values
    for doc in result:
        centroid = {'_id': count, 'kmeansNorm': doc['kmeansNorm']}
        db.centroids.insert_one(centroid)


# Run Code
create_centroids('Action', 5)
