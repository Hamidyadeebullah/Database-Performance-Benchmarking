import time
import redis
import os
import csv

redis_host = 'localhost'
redis_port = 6379
redis_db = 0

redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

if redis_client.ping():
    print("Connected to Redis")
else:
    print("Failed to connect to Redis")

datasets = [
    {'name': 'data_3ak', 'file_path': 'D:\\Database mod b project\\data_3ak.csv'},
    {'name': 'data_3bk', 'file_path': 'D:\\Database mod b project\\data_3bk.csv'},
    {'name': 'data_3ck', 'file_path': 'D:\\Database mod b project\\data_3ck.csv'},
    {'name': 'all_data', 'file_path': 'D:\\Database mod b project\\all_data.csv'}
]

base_directory = 'D:\\Database B\\Query results'

os.makedirs(base_directory, exist_ok=True)

def sanitize_query_description(description):
    return ''.join(char if char.isalnum() else '_' for char in description)

for dataset in datasets:
    set_name = dataset['name']
    print(f"Dataset: {set_name}")

    total_time = 0
    execution_times = []

    for query_index, query_template in enumerate([
        "admission_decision_approved_score_800_test_name_GRE",
        "test_name_SAT_degree_Office_manager",
        "institution_White_Inc",
        "all_data"
    ]):
        sanitized_query_description = sanitize_query_description(query_template)

        for i in range(30):
            start_time = time.perf_counter_ns()

            if query_index == 0:
                # Redis set intersection for the first query
                all_students = redis_client.smembers(f'student_{set_name}')
                approved_students = [
                    student for student in all_students
                    if int(redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'score').decode('utf-8')) > 800 and
                    redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'test_name').decode('utf-8') == 'GRE'
                ]
            elif query_index == 1:
                # Redis set intersection for the second query
                all_students = redis_client.smembers(f'student_{set_name}')
                office_manager_students = [
                    student for student in all_students
                    if redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'test_name').decode('utf-8') == 'SAT' and
                    redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'degree').decode('utf-8') == 'Office manager'
                ]
            elif query_index == 2:
                # Redis set intersection for the third query
                all_students = redis_client.smembers(f'student_{set_name}')
                white_inc_students = [
                    student for student in all_students
                    if redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'institution').decode('utf-8') == 'White Inc'
                ]
            elif query_index == 3:
                # Fetch all students for the fourth query
                all_students = redis_client.smembers(f'student_{set_name}')

            end_time = time.perf_counter_ns()
            execution_time = int(end_time - start_time)
            execution_times.append(execution_time)
            total_time += execution_time

            if i == 0:
                print(f"Query {query_index + 1}, First Execution Time: {execution_time} nanoseconds")
            if i == 29:
                avg_execution_time = total_time // 30
                print(f"Query {query_index + 1}, Average Execution Time: {avg_execution_time} nanoseconds")

    filename = os.path.join(base_directory, f"results_{set_name}.csv")
    with open(filename, 'w', newline='') as result_file:
        csv_writer = csv.writer(result_file)
        csv_writer.writerow(['Query', 'Execution Times'])
        for query_num in range(1, 5):
            query_label = f"Query {query_num}"
            response_times = execution_times[(query_num - 1) * 30: query_num * 30]
            csv_writer.writerow([query_label] + response_times)
