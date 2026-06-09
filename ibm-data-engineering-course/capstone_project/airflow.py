from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
from datetime import timedelta


# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Timothy May',
    'start_date': days_ago(0),
    'email': ['your email']
}

dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

def extract():
    print("Inside Extract")

    """
    This task should extract the ipaddress field from the web server log file and save it into a file named extracted_data.txt
    """

    # Each line in the Apache access log starts with the IP address.
    # We can split the line on spaces and take the first field.
    # This keeps memory usage low because we process one line at a time.

    with open("/home/project/accesslog.txt", "r") as infile:
        with open("/home/project/extracted_data.txt", "w") as outfile:
            for line in infile:
                ipaddress = line.split()[0]
                outfile.write(ipaddress + "\n")


def transform():
    print("Inside Transform")

    """
    This task should filter out all the occurrences of ipaddress "198.46.149.143" from extracted_data.txt and save the output to a file named transformed_data.txt.
    """

    # Read the extracted IPs one at a time.
    # Skip the IP we want to remove.
    # Write all remaining IPs to the transformed file.

    blocked_ip = "198.46.149.143"

    with open("/home/project/extracted_data.txt", "r") as infile:
        with open("/home/project/transformed_data.txt", "w") as outfile:
            for line in infile:
                ipaddress = line.strip()

                if ipaddress != blocked_ip:
                    outfile.write(ipaddress + "\n")


def load():
    print("Inside Load")
    """
    This task should archive the file transformed_data.txt into a tar file named weblog.tar.
    """
    # This function is not needed because the load task is handled by BashOperator.


extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag,
)

# for this i chose to use bash operator becuase its simple enough and gets the job done
# tar is already a Linux utility so there is no need to write Python code to archive the file
# Airflow's BashOperator is a good fit for simple shell commands like this

load_data = BashOperator(
    task_id='load_data',
    bash_command='tar -cf weblog.tar transformed_data.txt',
    dag=dag,
)

extract_data >> transform_data >> load_data