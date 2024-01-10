'''
Subject: INTRODUCTION TO BIG DATA
Assingment: 7
Author: Anirudh Narayanan
'''
import psycopg2
from itertools import combinations

# Function to create all the lattices required in the question.
def create_lattices():
    try:
        # Establish connection to database
        conn = psycopg2.connect(
            database="hw2_imdb",
            host="localhost")
        print("Connection Successful")

        # Create Cursor
        cur = conn.cursor()

        # Create and run queries and break out of loop if 0 itemsets are found.
        groups_found = i = 1
        while groups_found > 0:
            # Create Query by appending to a string.
            table = 'L' + str(i)
            query = "Create table L" + str(i) + " as Select * from (select"
            for k in range(1, i + 1):
                query += " pma" + str(k) + ".actor as actor" + str(k) + ","
            query += " count(pma" + str(i) + ".title) as movies from popular_movie_actors pma1"
            #  Perform these actions for all queries except for Table L1.
            if i != 1:
                arr = []
                for k in range(2, i + 1):
                    # Join table popular_movie_actor a number of times depending on the value of 'i'
                    query += " join popular_movie_actors pma" + str(k) + " on pma1.title = pma" + str(k) + ".title"
                k = 1
                query += " where"
                while k < i + 1:
                    arr.append(k)
                    k = k + 1
                # Create a list of combinations in order to make sure duplicate itemsets are not formed.
                result = list(combinations(arr, 2))
                combi = 0
                # add every combination in the list
                while combi < len(result):
                    query += " pma" + str(result[combi][0]) + ".actor < pma" + str(result[combi][1]) + ".actor"
                    if combi + 1 < len(result):
                        query += " and"
                    combi += 1
                # Check if the actor value exists in the previous table.
                query += " and exists (select"
                actor_len = 1
                while actor_len < i:
                    query += " actor" + str(actor_len)
                    if actor_len + 1 < i:
                        query += ","
                    actor_len += 1
                query += " from L" + str(i - 1) + ") group by ( "
                check_len = 1
                # Grouping by the number of actors required for the loop.
                while check_len < i + 1:
                    query += "actor" + str(check_len)
                    if check_len + 1 < i + 1:
                        query += ", "
                    check_len += 1
                query += ")) as t"
            # Only Append for L1
            else:
                query += " group by actor1) as t"
            query += " where movies > 4;"

            # Execute the query created and commit to the database.
            cur.execute(query)
            conn.commit()

            # Get number of itemsets in the lattice created and print it.
            query2 = "Select count(*) from L" + str(i) + ";"
            cur.execute(query2)
            result = cur.fetchone()
            groups_found = result[0]
            print("Created L" + str(i))
            print("Found " + str(groups_found) + " itemsets of actors for Level " + str(i) + ".")

            # Break out of loop of 0 itemsets formed.
            if groups_found == 0:
                print("Completed check for all Lattice Levels. Level 7 returns an empty table.")

            i = i + 1
        # Close the cursor and database connections
        cur.close()
        conn.close()

    except Exception as e:
        print("Error connection: ", e)


# Run function
create_lattices()
