import pandas as pd
import pymongo
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import time

# Connect to MongoDB
start = time.time()
client = pymongo.MongoClient()
db = client['movies']

# Get reference to collection
movies_collection = db['movies']

# 4.1
# For each genre, a five-number summary of the average ratings of movies with more than 10K votes.
# Create dictionary to keep track of the average rating of movie grouped by genres as keys.
movie_genre = {}

# Iterate over each document in the movies collection
for data in movies_collection.find():
    # Check if the required fields are present in the data
    if 'genres' in data and 'numvotes' in data and 'averagerating' in data:
        if data['numvotes'] > 10000:
            i = 0
            while i < len(data['genres']):
                if data['genres'][i] in movie_genre:
                    movie_genre[data['genres'][i]].append([data['averagerating']])
                else:
                    movie_genre[data['genres'][i]] = [[data['averagerating']]]
                i = i + 1

# Create dictionary to keep track of the 5 number summary of movie grouped by genres as key
movie_genre_five = {}
arr_check = []
# Iterate over keys of dictionary
for key in movie_genre:
    arr_check = movie_genre[key]
    # Create list  to store actors per movie by genres
    list1 = []
    for i in arr_check:
        for j in i:
            list1.append(float(j))
    # Find values to plot for the 5 number summary
    p25 = np.percentile(list1, 25)
    median = np.percentile(list1, 50)
    p75 = np.percentile(list1, 75)
    max1 = max(list1)
    min1 = min(list1)
    movie_genre_five[key] = [min1]
    movie_genre_five[key].append(p25)
    movie_genre_five[key].append(median)
    movie_genre_five[key].append(p75)
    movie_genre_five[key].append(max1)
'''
# print(movie_genre_five)
# Values Generated from above code
movie_genre_five ={'Comedy': [1.0, 6.0, 6.7, 7.4, 9.9],
 'Action': [1.1, 6.1, 6.8, 7.7, 9.9],
 'Adventure': [1.3, 6.2, 7.0, 7.9, 9.9],
 'Drama': [1.0, 6.5, 7.2, 7.8, 10.0],
 'Romance': [1.9, 6.1, 6.8, 7.4, 9.7],
 'Musical': [1.9, 6.5, 7.2, 7.5, 8.6], 
 'Horror': [1.6, 5.5, 6.2, 7.0, 9.8],
 'Sci-Fi': [1.5, 5.9, 6.6, 7.4, 9.8],
 'Mystery': [1.7, 6.1, 6.9, 7.6, 9.8],
 'Thriller': [1.2, 6.0, 6.7, 7.5, 10.0],
 'Biography': [1.0, 6.8, 7.2, 7.7, 9.3],
 'Crime': [1.2, 6.3, 7.1, 7.8, 10.0], 
 'Family': [1.3, 5.8, 6.6, 7.5, 9.7], 
 'Animation': [1.3, 6.8, 7.5, 8.225, 9.9],
 'Music': [1.5, 6.4, 7.0, 7.6, 9.0], 
 'Fantasy': [1.2, 5.9, 6.7, 7.5, 9.8], 
 'History': [3.3, 6.8, 7.3, 7.8, 9.8], 
 'War': [1.1, 6.8, 7.4, 7.9, 9.4], 
 'Documentary': [1.1, 7.4, 7.7, 8.1, 9.6], 
 'Western': [2.7, 6.7, 7.2, 7.6, 8.8],
 'Short': [5.3, 7.2, 7.6, 8.0, 9.2],
 'Sport': [2.2, 6.2, 6.9, 7.5, 8.8], 
 'Talk-Show': [5.3, 7.025, 8.1, 8.4, 8.9],
 'Game-Show': [4.2, 5.6499999999999995, 7.3, 8.325, 9.0], 
 'News': [7.2, 7.4, 8.2, 8.4, 8.9], 
 'Film-Noir': [6.6, 7.5, 7.7, 7.9, 8.4], 
 'Reality-TV': [2.8, 5.525, 7.15, 8.275, 9.0]

'''
# PloT THE DATA POINTS
df = pd.DataFrame(movie_genre_five)

# Set the color palette
num_genres = len(movie_genre_five)

# Create a dictionary with genre as key and corresponding color as value
genres = list(movie_genre_five.keys())
colors = sns.color_palette('hls', len(genres))

# Create subplots for each genre boxplot
fig, axes = plt.subplots(nrows=1, ncols=num_genres, figsize=(20, 4))
j = 0
for i, genre in enumerate(genres):
    if i ==0:
        ax = axes[i]
        sns.boxplot(ax=axes[i], data=df[[genre]], color=colors[j], width=0.4, whis=[0, 100])
    else:
        sns.boxplot(ax=axes[i], data=df[[genre]], color=colors[j], width=0.4, whis=[0, 100])
        axes[i].set_yticklabels([])
    axes[i].set_title(genre, fontsize=7)
    axes[i].set_xticklabels([])
    j = j + 1
# Set the labels for the figure
fig.suptitle('Boxplot for Different Genres')
fig.text(0.5, 0.04, 'Genres', ha='center')
fig.text(0.1, 0.5, 'Range', va='center', rotation='vertical')
for ax in axes:
    ax.tick_params(axis='y', labelsize=7)
    break
#print("Time Taken for AverageRating Boxplot: {:.2f}".format((end - start)) + "seconds")
plt.show()

