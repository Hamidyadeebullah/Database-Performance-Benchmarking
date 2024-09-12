import os
import csv
import mysql.connector
import time

base_directory = 'D:\\Database B\\Query results'

table_names = ['data_3ak', 'data_3bk', 'data_3ck', 'all_data']

queries = [
    "SELECT * FROM {} WHERE admission_decision = 'approved' AND score > 800 AND test_name = 'GRE'",
    "SELECT * FROM {} WHERE test_name = 'SAT' AND degree = 'Office manager'",
    "SELECT * FROM {} WHERE institution = 'White Inc'",
    "SELECT * FROM {}"
]

query_execution_times = {f"Query {i+1} execution times": [] for i in range(len(queries))}

connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="Adeeb1234",
    port="3306",
    database="admission",
    connect_timeout=120
)

cursor = connection.cursor()

for table_name in table_names:
    dataset_results = {f"Query {i+1}": [] for i in range(len(queries))}
    for i, query in enumerate(queries):
        formatted_query = query.format(table_name)
        execution_times = []
        for j in range(30):
            start_time = time.time()
            cursor.execute(formatted_query)
            for _ in cursor:
                pass
            end_time = time.time()
            execution_time = int((end_time - start_time) * 1e9)
            execution_times.append(execution_time)

            if j == 0:
                print(f"Table: {table_name}, Query {i+1}, First Execution Time: {execution_time} nanoseconds")
            if j == 29:
                avg_execution_time = sum(execution_times) // len(execution_times)
                print(f"Table: {table_name}, Query {i+1}, Average Execution Time: {avg_execution_time} nanoseconds")

        query_execution_times[f"Query {i+1} execution times"].append(execution_times)
        dataset_results[f"Query {i+1}"] = execution_times

    filename = os.path.join(base_directory, f"results_{table_name}.csv")
    with open(filename, 'w', newline='') as result_file:
        csv_writer = csv.writer(result_file)
        csv_writer.writerow(['Query', 'Execution Times'])
        for query_num in range(1, 5):
            query_label = f"Query {query_num}"
            response_times = dataset_results[query_label]
            csv_writer.writerow([query_label] + response_times)

cursor.close()
connection.close()
