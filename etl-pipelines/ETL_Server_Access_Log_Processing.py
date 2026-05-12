# Import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

# This makes scheduling easy
from airflow.utils.dates import days_ago
import requests

# Define the path for the input and output files
input_file = 'web-server-access-log.txt'
extracted_file = 'extracted-data.txt'
transformed_file = 'transformed.txt'
output_file = 'capitalized.txt'


def download_file():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    # Send a GET request to the URL
    with requests.get(url, stream=True) as response:
        # Raise an exception for HTTP errors
        response.raise_for_status()
        # Open a local file in binary write mode
        with open(input_file, 'wb') as file:
            # Write the content to the local file in chunks
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"File downloaded successfully: {input_file}")


def extract():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")


def transform():
    global extracted_file, transformed_file
    print("Inside Transform")
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as outfile:
        for line in infile:          
            processed_line = line.upper()
            outfile.write(processed_line + '\n')


def load():
    global transformed_file, output_file
    print("Inside Load")
    # Save the array to a CSV file
    with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(line + '\n')


def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)


# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Your name',
    'start_date': days_ago(0),
    'email': ['your email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'my-first-python-etl-dag',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

# Define the task named download to call the `download_file` function
download = PythonOperator(
    task_id='download',
    python_callable=download_file,
    dag=dag,
)

# Define the task named execute_extract to call the `extract` function
execute_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

# Define the task named execute_transform to call the `transform` function
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

# Define the task named execute_load to call the `load` function
execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

# Define the task named execute_load to call the `load` function
execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)

# Task pipeline
download >> execute_extract >> execute_transform >> execute_load >> execute_check


"""
wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xzf kafka_2.13-3.8.0.tgz

above version does not exist, so I downloaded below:
wget https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz
tar -xzf kafka_2.13-3.9.2.tgz
cd kafka_2.13-3.9.2

Generate a cluster UUID that will uniquely identify the Kafka cluster.
This cluster id will be used by the KRaft controller.
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

KRaft requires the log directories to be configured. 
Run the following command to configure the log directories passing the cluster ID.
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

Now that KRaft is configured, you can start the Kafka server by running the following command.
bin/kafka-server-start.sh config/kraft/server.properties

new terimnal
cd kafka_2.13-3.9.2
bin/kafka-topics.sh --create --topic news --bootstrap-server localhost:9092

You need a producer to send messages to Kafka. Run the command below to start a producer.
bin/kafka-console-producer.sh   --bootstrap-server localhost:9092   --topic news

After the producer starts, and you get the '>' prompt, type any text message and press enter. 
Or you can copy the text below and paste. 
The below text sends three messages to Kafka.

Good morning
Good day
Enjoy the Kafka lab

Start a new terminal and change to the kafka_2.13-3.8.0 directory.
cd kafka_2.13-3.8.0

You should see all the messages you sent from the producer appear here.
You can go back to the producer terminal and type some more messages, one message per line, and you will see them appear here.

Start a new terminal and navigate to the kafka_2.13-3.8.0 directory.
bin/kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic weather

Notice there is a tmp directory. The kraft-combine-logs inside the tmp directory contains all the logs. 
To check the logs generated for the topic news run the following command:
ls /tmp/kraft-combined-logs/news-0

Note: All messages are stored in the news-0 directory under the /tmp/kraft-combined-logs directory.


"""