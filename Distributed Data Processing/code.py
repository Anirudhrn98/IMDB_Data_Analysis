from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import col, collect_list, broadcast, exists, count, desc, split, array_contains
from pyspark.sql.types import IntegerType

# create a Session
spark = SparkSession.builder.appName("ASSN 9").config("spark.executor.memory", "4g").getOrCreate()

# Get data from tsv files
name_basics = spark.read.format("csv").option("delimiter", "\t").option("header", True).load(
    "/Users/anirudhramesh/Desktop/INTRO_TO_BIGDATA/ASSN9/Data/name_basics.tsv")
title_basics = spark.read.format("csv").option("delimiter", "\t").option("header", True).load(
    "/Users/anirudhramesh/Desktop/INTRO_TO_BIGDATA/ASSN9/Data/title_basics.tsv")
title_principals = spark.read.format("csv").option("delimiter", "\t").option("header", True).load(
    "/Users/anirudhramesh/Desktop/INTRO_TO_BIGDATA/ASSN9/Data/title_principals.tsv")

# title_basics.startYear.cast(IntegerType())
# title_basics.endYear.cast(IntegerType())


# QUERY 1
# Alive actors whose name starts with “Phi” and did not participate in any movie in 2014.
s1 = time.time()
q1_result = name_basics.join(title_principals, on="nconst", how="inner") \
    .select("primaryName", "nconst", "tconst", "primaryProfession", "deathYear") \
    .filter(((col("deathYear") == 'Null') | (col("deathYear") == '\\N')) &
            (col("primaryName").startswith('Phi')) & (col("primaryProfession")).contains('actor'))

q1_resultfinal = q1_result.join(title_basics, on="tconst", how="inner") \
    .select("primaryName", "startYear", "primaryTitle") \
    .filter((col("startYear").cast("int") != 2014))
q1_resultfinal.show(10)
print("Time taken for Query 1: " + str(time.time() - s1) + " seconds.")

# QUERY 2
# Producers who have produced the most talk shows in 2017 and whose name contains “Gill”.
s2 = time.time()
q2_result = name_basics.join(title_principals, on="nconst") \
    .select("nconst", "tconst", "primaryName").distinct() \
    .filter((col("primaryName").contains("Gill")) &
            (col("category") == "producer"))

q2_resultfinal = q2_result.join(title_basics, on="tconst") \
    .select("nconst", "tconst", "primaryName", "startYear", split("genres", ",").alias("Genre_array")) \
    .filter((col("startYear") == "2017"))

q2_output = q2_resultfinal.filter(array_contains(col("Genre_array"), "Talk-Show"))

q2_output2 = q2_output.groupBy(col("primaryName")) \
    .agg(count(col("tconst")).alias("Count")) \
    .sort(desc("Count"))

q2_output2.show(10)
print("Time taken for Query 2: " + str(time.time() - s2) + " seconds.")

# QUERY 3
# Alive producers with the greatest number of long-run titles produced (runtime greater than 120 minutes).
s3 = time.time()
q3_result = name_basics.join(title_principals, on="nconst") \
    .select("tconst", "nconst", "primaryProfession", "primaryName") \
    .filter(((col("deathYear") == 'Null') | (col("deathYear") == '\\N')) &
            (col("primaryProfession").contains('producer')) & (col("category") == "producer"))

q3_result2 = q3_result.join(title_basics, on="tconst") \
    .select("tconst", "primaryName") \
    .filter((col("runtimeMinutes").cast("int") > 120)).groupBy("primaryName") \
    .agg(count(col("tconst")).alias("Counts")) \
    .sort(desc("Counts"))

q3_result2.show(10)
print("Time taken for Query 3: " + str(time.time() - s3) + " seconds.")

# QUERY 4
# Alive actors who have portrayed Jesus Christ (look for both words independently).
s4 = time.time()
q4_result = name_basics.join(title_principals, on="nconst") \
    .select("primaryName", "characters") \
    .filter(((col("deathYear") == 'Null') | (col("deathYear") == '\\N')) &
            (col("primaryProfession").contains('actor')) & (col("category") == "actor") \
            & (col("characters").like("% Jesus %") |
               (col("characters").like("% Christ %"))))

q4_result.show(10)
print("Time taken for Query 4: " + str(time.time() - s4) + " seconds.")

spark.stop()
