import psycopg2
from itertools import *
import time


# main function
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
        arr = {}
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

                    # Prune approach
                    pruning1(col1d, col2d, arr, start_time)

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
                            print(f"Time taken for this check: {(end_time - start_time):.2f} seconds\n")
                            depency = False
                            break
                    if depency:             # if dependecy still true, it is a dependency
                        if col1d in arr.keys():
                            col2s = "" + arr[col1d] + "," + col2d
                            arr[col1d] = col2s
                        else:
                            arr[col1d] = col2d
                        print(f"{col1d} -> {col2d} is functionally dependent")
                        end_time = time.time()
                        print(f"Time taken for this check: {(end_time - start_time):.2f} seconds\n")
                    j = j + 1
            i = i + 1
            # CREATE ALL POSSIBLE COMBINATIONS ON LHS TO CHECK FOR DEPENDENCY (eg: (A,C) --> B, (A<B<C) --> D)
        for i in range(2, len(column_names)):
            for j in combinations(column_names, 2):
                columns_iter = ", ".join(j)
                prune_columns = columns_iter.split(',')
                lhs = rhs = 0

                while lhs < len(columns_iter):
                    while rhs < len(column_names):
                        depency2 = True
                        start_time2 = time.time()
                        col2d = column_names[rhs]
                        if col2d not in columns_iter:  # skip trivial dependency

                            prune_bool = pruning2(prune_columns, columns_iter, col2d, arr, start_time2)
                            if prune_bool != True:
                                my_dict1 = {}
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
                                    check = (col1[k][0], col1[k][1])
                                    if check not in my_dict1.keys():  # check keys ignore rest after
                                        my_dict1[check] = col2[k]
                                        continue
                                    elif check in my_dict1.keys() and col2[k] != my_dict1[check]:
                                        print(f"{columns_iter} -> {col2d} is not functionally dependent")
                                        arr_not.append([columns_iter, col2d])
                                        end_time2 = time.time()
                                        print(f"Time taken for this check: {(end_time2 - start_time2):.2f} seconds\n")
                                        depency2 = False
                                        break
                                if depency2:
                                    if columns_iter in arr.keys():
                                        col2s = "" + arr[columns_iter] + "," + col2d
                                        arr[columns_iter] = col2s
                                    else:
                                        arr[columns_iter] = col2d
                                        print(f"{columns_iter} -> {col2d} is functionally dependent")
                                        end_time = time.time()
                                        print(f"Time taken for this check: {(end_time - start_time2):.2f} seconds\n")
                                else:
                                    if columns_iter in arr.keys():
                                        col2s = "" + arr[columns_iter] + "," + col2d
                                        arr[columns_iter] = col2s
                            else:
                                if prune_bool == False:
                                    arr[columns_iter] = col2d
                                    print(f"{columns_iter} -> {col2d} is functionally dependent")
                                    end_time = time.time()
                                    print(
                                        f"Time taken for this check: {(end_time - start_time2):.2f} seconds using Trivial case\n")
                        else:
                            arr[columns_iter] = col2d
                            print(f"{columns_iter} -> {col2d} is functionally dependent using trivial case")
                            end_time = time.time()
                            print(f"Time taken for this check: {(end_time - start_time2):.2f} seconds\n")

                        rhs = rhs + 1
                    print(arr)
                    lhs = lhs + 1


        # Close connection and find time for executing dependency check
        cur.close()
        conn.close()
        end_time2 = time.time()

        print(f"Total time taken: {(end_time2 - st_time):.2f} seconds")
    except (Exception, psycopg2.Error) as error:

        print(error)


# FOR 1-1 Check
def pruning1(LHS, RHS, arr, start_time):
    k = 0
    while k < len(arr):
        st_search = [LHS + "','" + RHS]
        if LHS in arr.keys():
            if RHS in arr[LHS]:
                print(f"{LHS} -> {RHS} is functionally dependent using pruning approach.")
                arr[LHS] = RHS
                end_time = time.time()
                print(f"Time taken for this check: {(end_time - start_time):.2f} seconds\n")
                time.sleep(20)
                return True
            else:
                list = pruning_helper(arr[LHS])
                for i in list:
                    if i in arr.keys():
                        RHS = arr[i]
                        return pruning1(i, RHS, arr, start_time)
        k = k + 1
    return False


# for 2v1 check
def pruning2(LHS, LHS2, RHS, arr, start_time):
    k = 0
    while k < 2:
        if LHS[k] in arr.keys():
            if RHS in arr[LHS[k]] or RHS in arr[LHS2]:
                print(f"{LHS} -> {RHS} is functionally dependent using pruning approach.")
                end_time = time.time()
                print(f"Time taken for this check: {(end_time - start_time):.2f} seconds")
                if LHS2 in arr.keys():
                    col2s = "" + arr[LHS2] + "," + RHS
                    arr[LHS2] = col2s
                else:
                    arr[LHS2] = RHS

                return True
            else:
                list = pruning_helper(arr[LHS[k]])
                for i in list:
                    if i in arr.keys():
                        RHS = arr[i]
                        pruning2(i,i, RHS, arr, start_time)
        k = k + 1
    return False


# helper function
def pruning_helper(RHS):
    list = []
    str2 = ""
    for r in RHS:
        if r != ',':
            str2 = str2 + (r)
        elif r == ',':
            list.append(str2)
            str2 = ""
            continue
    list.append(str2)
    return list

execute_file()

