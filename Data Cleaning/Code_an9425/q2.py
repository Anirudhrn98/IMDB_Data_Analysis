import pymongo
from pymongo import MongoClient

# Connect to required MongoDB database
client = MongoClient()
db = client['movies']

# Get references to the collections
new_data_collection = db['new_data']
movies_collection = db['movies']

# variable to keep count of modifications
count = 0

# Iterate over the documents in the new data collection and update the value if the value is
# present in the collection
for data in new_data_collection.find():
    # Create dict to store information to update:
    new_values = {}
    # Store the values to update in the corresponding document in the movies collection
    if 'IMDb_ID' in data:
        new_values = {'_id': data['IMDb_ID']}
        if 'box_office_currencyLabel' in data:
            new_values['box_office_currencyLabel'] = data['box_office_currencyLabel']
        if 'cost' in data:
            new_values['cost'] = data['cost']
        if 'distributorLabel' in data:
            new_values['distributorLabel'] = data['distributorLabel']
        if 'MPAA_film_ratingLabel' in data:
            new_values['MPAA_film_ratingLabel'] = data['MPAA_film_ratingLabel']
        # update the values to the movies collection where the '_id' matches to 'IMDb_ID'
        update_res = movies_collection.update_one({'_id': data['IMDb_ID']}, {'$set': new_values})
        # Increment count
        count += update_res.modified_count

# Print the number of documents that were updated
print(f'{count} documents updated successfully based on IMDB ID.')
