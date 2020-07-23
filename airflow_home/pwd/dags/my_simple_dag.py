import datetime as dt
import json
import os
from datetime import datetime,timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from kafka import KafkaProducer

log = ""
UPLOAD_DIRECTORY = "./my_pipeline"
UPLOAD_DIRECTORY = os.path.abspath(UPLOAD_DIRECTORY)


def extract():
    """
    Fetch code from api for latest coronavirus data.
    :return: str, date , date for which the latest data has been collected.
    :return: int, number of rows stored in latest csv file in local system.
    """
    url = "https://api.covid19india.org/data.json"
    JSONContent = requests.get(url).json()

    my_list = []

    for i in JSONContent['statewise']:
        d = {'state': i['state'], 'count': i['confirmed'], 'date': datetime.now().date()}
        my_list.append(d)

    df = pd.DataFrame(data=my_list)
    # print(df.head(10))
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
    # print(df.head(10))
    df.to_csv(UPLOAD_DIRECTORY + '/reports/{}.csv'.format(datetime.now().date()), index=False)
    print("data added to csv with {} rows on {} at {}".format(len(df), datetime.now().date(), datetime.now().time()))
    global log
    log = "data added to csv with {} rows on {} at {}".format(len(df), datetime.now().date(), datetime.now().time())
    write_log()
    return datetime.now().date(), len(df) - 1


def write_log():
    """
    Method to write logs to file on local system.
    :return: None
    """
    log_list = [{'log': log}]
    df = pd.DataFrame(log_list)
    df.to_csv(UPLOAD_DIRECTORY + '/logs.csv', mode='a', header=False)


def load_data(file_date):
    """
    Method to load(append) data to big query partitioned table.
    :param file_date: str , name of file by date which has to be loaded
    :return: int , total rows uploaded into big query table
    """
    key_path = UPLOAD_DIRECTORY + "/My Project-3b4aa17f4859.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.schema_update_options = ['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION']
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 2
    # job_config.autodetect = True

    # creating a reference for the dataset in which the table is present
    dataset_ref = client.dataset('dataset_airflow_etl')

    print(file_date)
    date = str(file_date).replace('-', '')
    # creating a reference to the table
    table_ref = dataset_ref.table('airflow_table_assignment_divya${}'.format(date))

    filename = UPLOAD_DIRECTORY + '/reports/{}.csv'.format(file_date)
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    # print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_ref, table_ref))
    write_logs(job.output_rows, table_ref, dataset_ref)
    return job.output_rows


def write_logs(rows, table, dataset):
    """
    Method to write logs to kafka topic.
    :param rows: integer , number of rows added in current run
    :param table: string , name of bigquery table where the data has been added
    :param dataset: sring , name of the dataset where the bigquery table resides.
    :return: None
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    all_logs = []
    for i in range(5):
        d = {'timestamp': str(datetime.now()), 'log': "Loaded {} rows into {}:{}.".format(rows, dataset, table)}
        all_logs.append(json.dumps(d))

    if len(all_logs) > 0:
        kafka_producer = connect_kafka_producer()
        for log_msg in all_logs:
            publish_message(kafka_producer, 'pipeline_logs_topic', str(datetime.now().date()), log_msg)
        if kafka_producer is not None:
            kafka_producer.close()


def publish_message(producer_instance, topic_name, key, value):
    """
    Method to publish logs through the producer into the kafka topic.
    :param producer_instance: object , Kafka Producer instance to publish data
    :param topic_name: string , name of the topic into which data has to be published
    :param key: string , key against which data will be published into the topic
    :param value: dictionary , data to be published into the topic
    :return: None
    """
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    """
    Method to create an instance of Kafka producer after successful connection has been established.
    :return: object , instance of KafkaProducer class
    """
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def extract_report_func(**kwargs):
    """
    Method used by python operator to execute extraction for the pipeline.
    :param kwargs: dictionary , stores data flowing between operators in airflow
    :return: str , name of the latest extracted file
    """
    report_name = extract()
    return report_name


def load_data_func(**kwargs):
    """
    Method used by python operator to execute loading of data for the pipeline.
    :param kwargs: dictionary , stores data flowing between operators in airflow
    :return: int , total number of rows returned from load data method
    """
    ti = kwargs['ti']
    report_name, rows = ti.xcom_pull(task_ids='extract')
    output_rows = load_data(report_name)
    return output_rows


def get_percentage_func(**kwargs):
    """
    Method used by python operator to execute assignment calculation for the pipeline.
    :param kwargs: dictionary , stores data flowing between operators in airflow
    :return: double , percentage of successfully loaded data for the pipeline
    """
    ti = kwargs['ti']
    output_rows = ti.xcom_pull(task_ids='load')
    report_name, rows = ti.xcom_pull(task_ids='extract')
    return (output_rows * 100) / rows


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 6, 2, 7, 37, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('my_simple_dag',
         default_args=default_args,
         schedule_interval='*/2 * * * *',
         ) as dag:
    opr_hello = BashOperator(task_id='say_Hi',
                             bash_command='echo "Hi!!"')

    opr_extract = PythonOperator(task_id='extract',
                                 python_callable=extract_report_func,
                                 provide_context=True)

    opr_load = PythonOperator(task_id='load',
                              python_callable=load_data_func,
                              provide_context=True)

    opr_calc = PythonOperator(task_id='calc',
                              python_callable=get_percentage_func,
                              provide_context=True)

opr_hello >> opr_extract >> opr_load >> opr_calc
