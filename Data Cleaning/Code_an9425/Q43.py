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
# 4.3
movie_year = {}

# Iterate over each document in the movies collection
for data in movies_collection.find():
    # Check if the required fields are present in the data
    if "startyear" in data:
        if data['startyear'] not in movie_year:
            movie_year[data['startyear']] = 1
        else:
            movie_year[data['startyear']] = movie_year[data['startyear']] + 1

# Create sorted list of the yeara and corresponding number of movies
sorted_array = []
sorted_movie_year = {}
for key in movie_year:
    sorted_array.append(int(key))
    sorted_array = sorted(sorted_array)

# print(sorted_array)

for key in sorted_array:
    sorted_movie_year[key] = movie_year[str(key)]
    '''
sorted_movie_year = {1874: 1, 1877: 4, 1878: 3, 1881: 2, 1882: 2, 1883: 1, 1884: 1, 1885: 1, 1887: 45, 1888: 5,
              1889: 2, 1890: 6, 1891: 10, 1892: 9, 1893: 3, 1894: 99, 1895: 114, 1896: 853, 1897: 1356,
              1898: 1796, 1899: 1820, 1900: 1862, 1901: 1768, 1902: 1811, 1903: 2676, 1904: 1837,
              1905: 1704, 1906: 1858, 1907: 2489, 1908: 4299, 1909: 5437, 1910: 6418, 1911: 7748,
              1912: 8707, 1913: 9772, 1914: 9359, 1915: 8585, 1916: 7088, 1917: 5645, 1918: 4754,
              1919: 4220, 1920: 4565, 1921: 4268, 1922: 3664, 1923: 3075, 1924: 3160, 1925: 3468,
              1926: 3468, 1927: 3613, 1928: 3580, 1929: 3777, 1930: 3261, 1931: 3342, 1932: 3124,
              1933: 2954, 1934: 3004, 1935: 2936, 1936: 3384, 1937: 3609, 1938: 3439, 1939: 3061,
              1940: 2621, 1941: 2528, 1942: 2438, 1943: 2221, 1944: 2057, 1945: 2088, 1946: 2649,
              1947: 3046, 1948: 3845, 1949: 5239, 1950: 7564, 1951: 9646, 1952: 10426, 1953: 10377,
              1954: 12982, 1955: 13824, 1956: 13548, 1957: 15546, 1958: 16527, 1959: 16922, 1960: 18457,
              1961: 18906, 1962: 19058, 1963: 21350, 1964: 23124, 1965: 26711, 1966: 28272, 1967: 29615,
              1968: 28201, 1969: 32031, 1970: 31432, 1971: 32258, 1972: 32001, 1973: 32822, 1974: 31622,
              1975: 31756, 1976: 31557, 1977: 31271, 1978: 31735, 1979: 32221, 1980: 33519, 1981: 32591,
              1982: 35859, 1983: 36933, 1984: 37968, 1985: 39803, 1986: 39573, 1987: 42859, 1988: 41825,
              1989: 44884, 1990: 47870, 1991: 49746, 1992: 50707, 1993: 54988, 1994: 59269, 1995: 66747,
              1996: 70997, 1997: 76068, 1998: 85319, 1999: 90249, 2000: 93022, 2001: 102635, 2002: 105582,
              2003: 116327, 2004: 132846, 2005: 146851, 2006: 162620, 2007: 181576, 2008: 195024,
              2009: 206944, 2010: 236464, 2011: 268149, 2012: 304317, 2013: 325414, 2014: 345285,
              2015: 363615, 2016: 384812, 2017: 407495, 2018: 412553, 2019: 403784, 2020: 381785,
              2021: 436638, 2022: 381041, 2023: 71568, 2024: 804, 2025: 121, 2026: 25, 2027: 10,
              2028: 1, 2029: 3, 2030: 2}'''


# Create plots and plot time series plot
fig3, ax = plt.subplots()
# Plot the time series data
ax.plot(list(sorted_movie_year.keys()), list(sorted_movie_year.values()))

# Customize the x-axis tick marks and labels
start_year = min(sorted_movie_year.keys())
end_year = max(sorted_movie_year.keys())
ax.set_xticks(range(start_year, end_year + 1, 15))
ax.set_xticklabels(range(start_year, end_year + 1, 15), rotation=45)
# Set the x-axis label
ax.set_xlabel('Years')
end3 = time.time()
print("Time Taken for Plot: {:.2f}".format((end3 - start)) + "seconds")
# Set the y-axis label
ax.set_ylabel('Number of Movies')

# Set the plot title
ax.set_title('Movie Production by Year')

# Show the plot
plt.show()
