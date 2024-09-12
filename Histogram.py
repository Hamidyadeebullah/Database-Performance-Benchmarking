import csv
import matplotlib.pyplot as plt
import numpy as np

def process_data(data):
    first_values = []
    averages = []

    for sublist in data:
        first_value = float(sublist[0])
        first_values.append(first_value)

        remaining_values = [float(value) for value in sublist[1:]]
        average = sum(remaining_values) / len(remaining_values)
        averages.append(average)

    return first_values, averages

def plot_bar_plot(datasets, response_times, xlabel, ylabel, title):
    plt.figure(figsize=(4, 5))
    plt.bar(datasets, response_times, width=0.1)
    plt.yscale('log')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    plt.show()

def plot_grouped_bar_plot(response_times, databases, dataset_sizes, xlabel, ylabel, title):
    plt.figure(figsize=(10, 10))  # Adjust the figure size
    bar_width = 0.15
    space_between_bars = 0
    index = np.arange(len(response_times[0]))

    for i, times in enumerate(response_times):
        # Convert response times from nanoseconds to milliseconds with 3 decimal places
        times_ms = [round(val / 1e6, 3) for val in times]
        plt.bar(index + (bar_width + space_between_bars) * i, times_ms,
                bar_width, alpha=0.7, label=f'Dataset {dataset_sizes[i]}')

    plt.yscale('log')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)

    plt.xticks([])  # Remove x-axis labels

    plt.legend()
    plt.grid(True)

    # Adding a table with the exact numbers
    cell_text = []
    for i, times in enumerate(response_times):
        # Convert response times from nanoseconds to milliseconds with 3 decimal places for table
        times_ms_table = [round(val / 1e6, 3) for val in times]
        row = [f'{val:.3f}' for val in times_ms_table]
        cell_text.append(row)

    table = plt.table(cellText=cell_text,
                      rowLabels=[f'Dataset {dataset_sizes[i]}' for i in range(len(response_times))],
                      colLabels=databases,
                      loc='bottom')

    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.5)  # Adjust the table size as needed

    plt.tight_layout()  # Ensure that everything fits within the figure
    plt.show()

file_mysql = [
    "D:\\Database B\\Query results\\mysql query results\\results_data_3ak.csv",
    "D:\\Database B\\Query results\\mysql query results\\results_data_3bk.csv",
    "D:\\Database B\\Query results\\mysql query results\\results_data_3ck.csv",
    "D:\\Database B\\Query results\\mysql query results\\results_all_data.csv"
]

allValues_mySql = []

for file in file_mysql:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        second_row = next(csv_reader)
        values = second_row[1:]
        allValues_mySql.append(values)

firstvalue_mySql, average_of_theRest = process_data(allValues_mySql)


file_redis = [
    "D:\\Database B\\Query results\\redis query result\\results_data_3ak.csv",
    "D:\\Database B\\Query results\\redis query result\\results_data_3bk.csv",
    "D:\\Database B\\Query results\\redis query result\\results_data_3ck.csv",
    "D:\\Database B\\Query results\\redis query result\\results_all_data.csv"
]

allValues_redis = []

for file in file_redis:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        second_row = next(csv_reader)
        values = second_row[1:]
        allValues_redis.append(values)

firstvalue_redis, average_of_theRest_redis = process_data(allValues_redis)


file_cassandra = [
    "D:\\Database B\\Query results\\cassandra query results\\results_250k.csv",
    "D:\\Database B\\Query results\\cassandra query results\\results_500k.csv",
    "D:\\Database B\\Query results\\cassandra query results\\results_750k.csv",
    "D:\\Database B\\Query results\\cassandra query results\\results_overall_data.csv"
]

allValues_cassandra = []

for file in file_cassandra:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        second_row = next(csv_reader)
        values = second_row[1:]
        allValues_cassandra.append(values)

firstvalue_cassandra, average_of_theRest_cassandra = process_data(allValues_cassandra)


file_mongodb = [
    "D:\\Database B\\Query results\\mongodb query\\results_data_3ak.csv",
    "D:\\Database B\\Query results\\mongodb query\\results_data_3bk.csv",
    "D:\\Database B\\Query results\\mongodb query\\results_data_3ck.csv",
    "D:\\Database B\\Query results\\mongodb query\\results_all_data.csv"
]

allValues_mongodb =[]

for file in file_mongodb:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        second_row = next(csv_reader)
        values = second_row[1:]
        allValues_mongodb.append(values)

firstvalue_mongodb, average_of_theRest_mongodb = process_data(allValues_mongodb)


file_neo4j = [
    "D:\\Database B\\Query results\\neo4j query result\\results_data_3ak.csv",
    "D:\\Database B\\Query results\\neo4j query result\\results_data_3bk.csv",
    "D:\\Database B\\Query results\\neo4j query result\\results_data_3ck.csv",
    "D:\\Database B\\Query results\\neo4j query result\\overall_data.csv"
]

allValues_neo4j = []

for file in file_neo4j:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        second_row = next(csv_reader)
        values = second_row[1:]
        allValues_neo4j.append(values)

firstvalue_neo4j, average_of_theRest_neo4j = process_data(allValues_neo4j)

dataset_sizes = ["250k", "500k", "750k", "1m"]

response_times = [
    [firstvalue_mySql[0], firstvalue_redis[0], firstvalue_cassandra[0], firstvalue_mongodb[0], firstvalue_neo4j[0]],
    [firstvalue_mySql[1], firstvalue_redis[1], firstvalue_cassandra[1], firstvalue_mongodb[1], firstvalue_neo4j[1]],
    [firstvalue_mySql[2], firstvalue_redis[2], firstvalue_cassandra[2], firstvalue_mongodb[2], firstvalue_neo4j[2]],
    [firstvalue_mySql[3], firstvalue_redis[3], firstvalue_cassandra[3], firstvalue_mongodb[3], firstvalue_neo4j[3]]
]

# Plotting grouped bar plot with table
plot_grouped_bar_plot(response_times, ['mySql', 'redis', 'cassandra', 'mongodb', 'neo4j'], dataset_sizes, '', 'Response Time (ms)',
                      'Response Time for Different Databases and Datasets')
