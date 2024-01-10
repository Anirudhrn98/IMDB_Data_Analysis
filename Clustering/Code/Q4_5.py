"""
Subject: INTRODUCTION TO BIG DATA
Assingment : 7-Q4
Author: Anirudh Narayanan
"""
import pymongo
import math
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


#  Q2
# Create centroids by picking k number of random documents from the collection
def create_centroids(g, k):
    # Create connection
    client = pymongo.MongoClient()
    db = client['Clustering']
    if 'centroids' in db.list_collection_names():
        db.centroids.drop()

    query = [{'$match': {'$and': [{'kmeansNorm': {'$exists': True}},
                                  {'genres': {"$in": [g]}}]}},
             {'$sample': {'size': k}}]

    result = db.movies.aggregate(query)
    count = 1
    for doc in result:
        centroid = {'_id': count, 'kmeansNorm': doc['kmeansNorm']}
        db.centroids.insert_one(centroid)
        count += 1


# Q3 - Function to assign new clusters to each point of a particular genre on the basis of the new centroids found
# in the previous iteration.
def assign_clusters(g, j):
    # Create connection
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
    # Create connection
    client = pymongo.MongoClient()
    db = client['Clustering']
    centroids = db.get_collection('centroids')

    cluster_mov = db.get_collection('clusters_movies')
    cluster_mov.create_index('cluster')
    cluster_mov.create_index('kmeansNorm')
    centroids.create_index("kmeansNorm")
    # Create dictionary and store centroid information
    centroidsl = {}
    for cent in centroids.find():
        centroidsl[cent['_id']] = cent['kmeansNorm']
    # Find New Centroids
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

    return current_cents


# Q4 - Function to find sum of squared errors.
def find_sse(g):
    # Create connection
    client = pymongo.MongoClient()
    db = client['Clustering']
    centroids = db.get_collection('centroids')
    centroidsl = {}
    for cent in centroids.find():
        centroidsl[cent['_id']] = cent['kmeansNorm']
    movies_col = db.get_collection('movies')
    sse = 0
    for movie in movies_col.find({'$and': [{'cluster': {'$exists': True}}, {'genres': {"$in": [g]}}]}):
        sse += math.dist(movie['kmeansNorm'], centroidsl[movie['cluster']]) ** 2
    return sse


def create_cluster_graph(i):
    # Create connection
    client = pymongo.MongoClient()
    db = client['Clustering']
    movies = db.get_collection('clusters_movies')
    cluster_movie = {}
    for movie in movies.find():
        if movie['cluster'] in cluster_movie.keys():
            cluster_movie[movie['cluster']].append(movie['kmeansNorm'])
        else:
            cluster_movie[movie['cluster']] = [movie['kmeansNorm']]
    # loop through each cluster and plot the kmeansNorm values
    plt.figure(figsize=(8, 8))
    for cluster, values in cluster_movie.items():
        xs = [v[0] for v in values]
        ys = [v[1] for v in values]
        sns.scatterplot(x=xs, y=ys, label=f"Cluster {cluster}", legend=False)
    plt.xlabel("KmeansNorm[0]")
    plt.ylabel("KmeansNorm[1]")
    plt.title("Cluster Movie Scatter Plot Genre:Thriller")
    plt.savefig("Graph_" + i + ".png")
    # plt.show(block=False)


# Q4

def run_kmeans():
    # List of Genres to run k-means for
    genres = ['Action', 'Horror', 'Romance', 'Sci-Fi', 'Thriller']
    for i in genres:
        # k value from 10 to 50
        k = 10
        sse_k = {}
        while k < 51:
            # Create array to store centroid values to check for convergence
            old_cents = []
            j = 0
            create_centroids(i, k)
            convergence = False
            while j <= 100:
                print("Iteration " + str(j))
                assign_clusters(i, j)
                cents = find_new_centroids()
                # if reached convergence
                if old_cents == cents:
                    print("Reached convergence after " + str(j) + " iterations with " + str(
                        k) + " centroids for " + i + " genre.")
                    convergence = True
                    break
                old_cents = cents
                j = j + 1
            if not convergence:
                print("No convergence after 100 iterations for " + str(k) + " centroids for " + i + " genre. ")

            # Find Sum of Squared errors using helper function
            sse_k[k] = find_sse(i)
            k += 5
        # Plot sse vs k plot
        plt.clf()
        plt.plot(sse_k.keys(), sse_k.values())
        plt.title(f'{i} genre')
        plt.xlabel('k')
        plt.ylabel('Sum of Squared Errors')
        plt.savefig(i + '.png')
        # plt.show(block=False)


def Q_5_helper(g, k):
    i = g
    if k < 51:
        old_cents = []
        j = 0
        create_centroids(i, k)
        convergence = False
        while j <= 100:
            print("Iteration " + str(j))
            assign_clusters(i, j)
            cents = find_new_centroids()
            if old_cents == cents:
                print("Reached convergence after " + str(j) + " iterations with " + str(
                    k) + " centroids for " + i + " genre.")
                print()
                convergence = True
                create_cluster_graph(i)
                break
            old_cents = cents
            j = j + 1
        if not convergence:
            print("No convergence after 100 iterations for " + str(k) + " centroids for " + i + " genre. ")


def Q_5():
    # Run Q5 code

    print("Executing Action Genre...")
    print()
    Q_5_helper('Action', 35)
    print("Executing Horror Genre...")
    print()
    Q_5_helper('Horror', 20)
    print("Executing Romance Genre...")
    print()
    Q_5_helper('Romance', 35)
    print("Executing Sci-Fi Genre...")
    print()
    Q_5_helper('Sci-Fi', 40)
    print("Executing Thriller Genre...")
    print()
    Q_5_helper('Thriller', 30)
    print("Done executing Q4 and Q5 and save all the plots.")


# Run Code
print("Executing Q4:")
run_kmeans()
print("Finisihed running k-means.")
print()
print("Executing Q5:")
Q_5()
