"""
Subject: INTRODUCTION TO BIG DATA
Assingment : 7-Q2
Author: Anirudh Narayanan
"""
import pymongo
import math
import matplotlib.pyplot as plt


# Q3 - Function to assign new clusters to each point of a particular genre on the basis of the new centroids found
# in the previous iteration.
def assign_clusters(g, j):
    client = pymongo.MongoClient()
    db = client['Clustering']
    centroids_coll = db.get_collection('centroids')
    # Create dictionary and store centroid information
    centroids = {}
    for cent in centroids_coll.find():
        centroids[cent['_id']] = cent['kmeansNorm']
    movies_col = db.get_collection('movies')
    # Index to speed up
    movies_col.create_index('kmeansNorms')
    movies_col.create_index('genres')

    # Loop over each movie document and find the closest cluster and assign new cluster value to the document
    # in the mongo collection.
    if j == 0:
        if 'clusters_movies' in db.list_collection_names():
            db.clusters_movies.drop()
        for movie in movies_col.find({'$and': [{'kmeansNorm': {'$exists': True}}, {'genres': {"$in": [g]}}]}):
            norm_values = movie['kmeansNorm']
            counter = 1
            centroid_dist = []
            for i in centroids:
                dist = math.dist([norm_values[0], norm_values[1]], [centroids[i][0], centroids[i][1]])
                centroid_dist.append(dist)
                counter += 1
            closest_cluster = centroid_dist.index(min(centroid_dist)) + 1  # cluster = index + 1
            db.movies.update_one({'_id': movie['_id']}, {'$set': {'cluster': closest_cluster}})
            db.clusters_movies.insert_one(
                {'_id': movie['_id'], 'cluster': closest_cluster, 'kmeansNorm': movie['kmeansNorm']})
    else:
        cluster_mov = db.get_collection('clusters_movies')
        cluster_mov.create_index('cluster')
        cluster_mov.create_index('kmeansNorm')
        for movie in cluster_mov.find():
            norm_values = movie['kmeansNorm']
            counter = 1
            centroid_dist = []
            for i in centroids:
                dist = math.dist([norm_values[0], norm_values[1]], [centroids[i][0], centroids[i][1]])
                centroid_dist.append(dist)
                counter += 1
            closest_cluster = centroid_dist.index(min(centroid_dist)) + 1  # cluster = index + 1
            db.movies.update_one({'_id': movie['_id']}, {'$set': {'cluster': closest_cluster}})
            db.clusters_movies.update_one(
                {'_id': movie['_id']},
                {'$set': {'cluster': closest_cluster, 'kmeansNorm': movie['kmeansNorm']}}
            )


# Function to find new centroids based on the distance of each point from the cluster centroid.
def find_new_centroids():
    # Create Connection
    client = pymongo.MongoClient()
    db = client['Clustering']
    centroids = db.get_collection('centroids')
    cluster_mov = db.get_collection('clusters_movies')

    # Index to speed up
    cluster_mov.create_index('cluster')
    cluster_mov.create_index('kmeansNorm')
    centroids.create_index("kmeansNorm")

    # Create dictionary and store centroid information
    centroidsl = {}
    for cent in centroids.find():
        centroidsl[cent['_id']] = cent['kmeansNorm']

    # Find New Centroids and update centroid collection
    current_cents = []
    for i in centroidsl:
        sumx = [0, 0]
        count_movies = 0
        for movie in cluster_mov.find():
            if movie['cluster'] == i:
                sumx[0] += movie['kmeansNorm'][0]
                sumx[1] += movie['kmeansNorm'][1]
                count_movies += 1
        # if there are movies in the cluster, then proceed.
        if count_movies != 0:
            n_center = [float(sumx[0] / count_movies), float(sumx[1] / count_movies)]
            db.centroids.update_one({'_id': i}, {'$set': {'kmeansNorm': n_center}})
            current_cents.append(n_center)
        # print(str(n_center) + " is the new centroid for cluster " + str(i) + " for " + g + ".")
    # print()
    # print(current_cents)


# Run Code
assign_clusters('Action', 0)
find_new_centroids()