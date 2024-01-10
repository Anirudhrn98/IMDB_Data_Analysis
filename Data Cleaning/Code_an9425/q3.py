import pymongo
from pymongo import MongoClient
import time

# Connect to MongoDB
start = time.time()
client = MongoClient()
db = client['movies']

# Get references to the collections
new_data_collection = db['new_data']
movies_collection = db['movies_backup']  # Back_up movies collection

# Creating indexes
movies_collection.create_index("primarytitle")
movies_collection.create_index("original_title")

# Part 1 - Not keeping track of Titles.
# Keep count of modifications
count_mod = 0
title_updated = {}
# Iterate over the documents in the new data collection
for data in new_data_collection.find():
    # Find and update the document in movies collection to add field titleLabel if 'titleLabel' matches to the
    # 'primarytitle' or 'original_title'
    if 'titleLabel' in data:
            update_result = movies_collection.update_many(
                {'$or': [{'primarytitle': data['titleLabel']},
                         {'original_title': data['titleLabel']}]},
                {'$set': {'titleLabel': data['titleLabel']}})
            count_mod += update_result.modified_count
end = time.time()

# Print time taken and number of documents updated
print("Time Taken: {:.2f}".format((end - start)) + "seconds")
print("Updated " + str(count_mod) + " documents in movies_backup collection using titleLabel.")

# Part 2 - Keeping track of Title Updated
'''
count_mod2 = 0
title_updated = {}
# Iterate over the documents in the new data collection
for data in new_data_collection.find():
    # Find and update the document in movies collection to add field titleLabel if 'titleLabel' matches to the 
    # 'primarytitle' or 'original_title' and if the 'titleLabel' has not been processed. 
    if 'titleLabel' in data:
        if data['titleLabel'] not in title_updated:
            update_result = movies_collection.update_one(
                {'$or': [{'primarytitle': data['titleLabel']},
                         {'original_title': data['titleLabel']}]},
                {'$set': {'titleLabel': data['titleLabel']}})
            count_mod2 += 1
            title_updated[data['titleLabel']] = True  # keeping track of titles already visited

end2 = time.time()
print("Time Taken: {:.2f}".format((end2 - start)) + "seconds")
print("Updated " + str(count_mod2) + "  movies_backup collection using titleLabel.")
'''
