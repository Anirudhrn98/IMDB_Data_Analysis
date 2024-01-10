"""
Subject: INTRODUCTION TO BIG DATA
Assingment : 7-Q1
Author: Anirudh Narayanan
"""
import pymongo


def normalize_append():
    # Create Connection
    client = pymongo.MongoClient()
    db = client['Clustering']
    movies_unfiltered = db['movies']

    # Index to speed up
    movies_unfiltered.create_index('averagerating')
    movies_unfiltered.create_index('startyear')

    # Define array to store values
    movies_filtered = []
    avgratings = []
    startyears = []

    # Filter out documents where numVotes > 10000 , average rating and startYear exist
    for movie in movies_unfiltered.find({'numvotes': {'$gt': 10000},
                                         'averagerating': {'$exists': True},
                                         'startyear': {'$exists': True}}):
        avgratings.append(movie['averagerating'])
        startyears.append(int(movie['startyear']))
        movies_filtered.append(movie)

    # Find min and max values to compute normalized values
    min_rating = min(avgratings)
    max_rating = max(avgratings)
    min_startyear = min(startyears)
    max_startyear = max(startyears)
    print("Computed minimum and maximum values.")
    print("Now creating kmeansNorms field")
    print("Creating..........")

    # Compute normalized values
    count = 0
    for movie in movies_filtered:
        normalized_field = []
        yearx = int(movie['startyear'])
        normalized_field.append((yearx - min_startyear) / (max_startyear - min_startyear))
        avgratingx = movie['averagerating']
        normalized_field.append(float((avgratingx - min_rating) / (max_rating - min_rating)))
        db.movies.update_one({'_id': movie['_id']}, {'$set': {'kmeansNorm': normalized_field}})
        count += 1

    print("Created kmeansNorm field in " + str(count) + " documents.")
    # Created kmeansNorm field in 13800 documents.


# Run Code
normalize_append()
