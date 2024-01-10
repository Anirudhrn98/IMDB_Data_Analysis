import psycopg2
from itertools import *
import time


def execute_file():
    try:
        # STORE START TIME OF EXECUTION
        st_time = time.time()
        # Establish connection to postgres db
        conn = psycopg2.connect(
            host="localhost",
            database="hw2_imdb"
        )
        conn.autocommit = True
        cur = conn.cursor()
        arr_not = []
        arr = []
        # Get columns names
        sql = '''SELECT * FROM Q1'''
        cur.execute(sql)
        column_names = [desc[0] for desc in cur.description]

        # CHECK ALL A --> B Dependencies (1-1 checks)

        i = 0

        while i < len(column_names):
            j = i + 1
            while j < len(column_names):
                start_time = time.time()
                if i != j:  # Skip checking rows with itself
                    my_dict = {}  # create dictionary to store each loop value as a (key,value) pair
                    depency = True

                    # Access and store column information using sql query
                    col1d = column_names[i]
                    col2d = column_names[j]
                    sql1 = f"SELECT {col1d} FROM Q1"
                    cur.execute(sql1)
                    col1 = cur.fetchall()
                    sql2 = f"SELECT {col2d} FROM Q1"
                    cur.execute(sql2)
                    col2 = cur.fetchall()
                    len_col = 0

                    # Process data
                    while len_col < len(col1):
                        col1[len_col] = col1[len_col][0]
                        col2[len_col] = col2[len_col][0]
                        len_col = len_col + 1

                    # CHECK ALL 1-1 dependency check
                    for k in range(len(col1)):
                        if col1[k] not in my_dict.keys():  # check keys ignore rest after
                            my_dict[col1[k]] = col2[k]
                        elif col1[k] in my_dict.keys() and col2[k] != my_dict[col1[k]]:
                            print(f"{col1d} -> {col2d} is not functionally dependent")
                            arr_not.append([col1d, col2d])
                            end_time = time.time()
                            print(f"Time taken for this check: {(end_time - start_time):.2f} seconds")
                            depency = False
                            break

                    if depency:
                        print(f"{col1d} -> {col2d} is functionally dependent")
                        arr.append([col1d, col2d])
                        end_time = time.time()
                        print(f"Time taken for this check: {(end_time - start_time):.2f} seconds")

                j = j + 1
            i = i + 1 


        # CREATE ALL POSSIBLE COMBINATIONS ON LHS TO CHECK FOR DEPENDENCY (eg: (A,C) --> B, (A<B<C) --> D)
        for i in range(2, len(column_names)):
            for j in combinations(column_names, i):
                columns_iter = ", ".join(j)
                lhs = rhs = 0
                while lhs < len(columns_iter):
                    while rhs < len(column_names):
                        depency2 = True
                        start_time2 = time.time()
                        my_dict1 = {}
                        col2d = column_names[rhs]
                        if col2d not in columns_iter:  # skip trivial dependency
                            sql1 = f"SELECT {columns_iter} FROM Q1 GROUP BY {columns_iter}"
                            cur.execute(sql1)
                            col1 = cur.fetchall()
                            sql2 = f"SELECT {col2d} FROM Q1"
                            cur.execute(sql2)
                            col2 = cur.fetchall()
                            len_col = 0
                            # Process data
                            while len_col < len(col2):
                                col2[len_col] = col2[len_col][0]
                                len_col = len_col + 1
                            for k in range(len(col1)):
                                if col1[k] not in my_dict1.keys():  # check keys ignore rest after
                                    my_dict1[col1[k]] = col2[k]
                                    continue
                                elif col1[k] in my_dict1.keys() and col2[k] != my_dict1[col1[k]]:
                                    print(f"{columns_iter} -> {col2d} is not functionally dependent")
                                    arr_not.append([columns_iter, col2d])
                                    end_time2 = time.time()
                                    print(f"Time taken for this check: {(end_time2 - start_time2):.2f} seconds")
                                    depency2 = False
                                    break
                            if depency2:
                                print(f"{columns_iter} -> {col2d} is functionally dependent")
                                arr.append([columns_iter, col2d])
                                end_time2 = time.time()
                                print(f"Time taken for this check: {(end_time2 - start_time2):.2f} seconds")
                        else:
                            print(f"{columns_iter} -> {col2d} is functionally dependent")
                            arr.append([columns_iter, col2d])
                            end_time2 = time.time()
                            print(f"Time taken for this check: {(end_time2 - start_time2):.2f} seconds. Trivial case")

                        rhs = rhs + 1

                    lhs = lhs + 1

        cur.close()
        conn.close()
        end_time2 = time.time()
        print(f"Total time taken: {(end_time2 - st_time):.2f} seconds")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)






execute_file()
