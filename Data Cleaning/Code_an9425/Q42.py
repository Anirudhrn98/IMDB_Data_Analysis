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

# Create dictionary to keep track of the number of actors per movie grouped by genres as keys.
actor_genre = {}
# Iterate over each document in the collection and find documents where actor and genre
# fields are present and store the length of the actors array for each genre in the genre field.
for data in movies_collection.find():
    # Check if the required fields are present in the data
    if 'actors' in data and 'genres' in data:
        i = 0
        while i < len(data['genres']):
            if data['genres'][i] in actor_genre.keys():
                actor_genre[data['genres'][i]].append(len(data['actors']))
            else:
                actor_genre[data['genres'][i]] = [len(data['actors'])]
            i = i + 1


# Find average number of actors for each genre
for key in actor_genre:
    arr_check = actor_genre[key]
    average_arr = [sum(arr_check) / len(arr_check)]
    actor_genre[key] = average_arr
'''
# print(actor_genre)
actor_genre = {'Drama': 2.496203711477821, 'Crime': 2.845294566128604, 'Horror': 2.3950656195554156,
 'Mystery': 2.6940379213483148, 'Comedy': 2.576592253425935, 'Adventure': 2.9266288642079448,
  'Thriller': 2.5782872008852022, 'Action': 3.1299378460216603, 'Animation': 2.5845648419735605,
   'Romance': 2.427722916104627, 'Family': 2.436253397334736, 'Short': 2.20165231263678,
    'History': 2.886804895255038, 'Sci-Fi': 2.7859257301440463, 'Music': 2.1053644482857967, 
    'Musical': 2.2783266082278186, 'Documentary': 1.6706308374228274, 'Fantasy': 2.5125972516383914, 
    'Reality-TV': 1.5234693718054926, 'Western': 3.419810369174904, 'War': 3.2755886359213853,
     'Biography': 2.4905052691247325, 'Film-Noir': 2.5548022598870057, 'Game-Show': 2.0119546071438883,
      'Sport': 3.7707458906839872, 'News': 1.5732114904598722, 'Talk-Show': 1.5693338495984916,
       'Adult': 2.062937062937063}'''

# Create a dataframe from the dictionary
df = pd.DataFrame.from_dict(actor_genre, orient='index', columns=['avg_act'])

# Create a bar plot
fig, ax = plt.subplots(figsize=(8, 10))
sns.barplot(x='avg_act', y=df.index, data=df, ax=ax, color='lightblue')

# Set the labels for the figure
plt.title('Average Number of Actors per Genre')
plt.xlabel('Average Number of Actors')
plt.ylabel('Genres')

# Show plot
plt.show()



