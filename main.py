from neo4j import GraphDatabase
import pandas as pd
import schedule
import time

uri = "bolt://localhost:7687"
username = "neo4j"
password = "Adeeb1234"

# Cypher query to create nodes for each row in the CSV
create_query = """
CREATE (student:data_3ck{
    id: $id,
    name: $name,
    email: $email,
    phone_number: $phone_number,
    address: $address,
    application_date: $application_date,
    admission_decision: $admission_decision,
    institution: $institution,
    degree: $degree,
    grade: $grade,
    test_name: $test_name,
    score: $score
})
"""

# Load CSV data into a Pandas DataFrame
csv_file_path = "D:/Database mod b project/data_3ck.csv"  # Update with the path to your CSV file
data_frame = pd.read_csv(csv_file_path)

# Counter for tracking the number of inserted records
inserted_records = 0

# Function to insert data into Neo4j
def insert_data():
    global inserted_records
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            for _, row in data_frame.iterrows():
                session.run(create_query, row.to_dict())
                inserted_records += 1
                print(f"Inserted record with ID {row['id']}. Total records: {inserted_records}")

# Function to log the progress
def log_progress():
    print(f"{inserted_records} records inserted.")

# Schedule data insertion once
schedule.every(1).minutes.do(insert_data)
# Schedule progress logging once
schedule.every(1).minutes.do(log_progress)

# Run scheduled tasks
schedule.run_all()

# Wait for the scheduled tasks to complete
while schedule.get_jobs():
    time.sleep(1)

print("Data insertion completed.")
