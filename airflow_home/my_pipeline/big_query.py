""" BigQuery data accessing."""
import os

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

UPLOAD_DIRECTORY = "./my_pipeline"
UPLOAD_DIRECTORY = os.path.abspath(UPLOAD_DIRECTORY)


class BigQueryData:
    """
    Class to create dataset and table and access data from it.
    """

    def __init__(self):
        """
        Constructor for class to initialise values to create client.
        """
        # please insert path to your credentials file.
        self.key_path = "My Project-3b4aa17f4859.json"

        self.credentials = service_account.Credentials.from_service_account_file(
            self.key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id,
        )

    def create_dataset(self):
        """
        Method to create a dataset in the service account.
        """
        # creating a reference for the required dataset
        dataset_ref = self.client.dataset('dataset_airflow_etl')

        try:
            # fetching the dataset with given reference
            self.client.get_dataset(dataset_ref)
        except NotFound:
            # if dataset not already present then create a new dataset
            dataset = bigquery.Dataset(dataset_ref)
            dataset = self.client.create_dataset(dataset)
            print('Dataset {} created.'.format(dataset.dataset_id))

    def create_table(self):
        """
        Method to create a table in the dataset with schema and data from a csv.
        """
        # creating a reference for the dataset in which the table is present
        dataset_ref = self.client.dataset('dataset_airflow_etl')

        # creating a reference to the table
        table_ref = dataset_ref.table('airflow_table_assignment_divya')

        try:
            # fetching details of the table from the reference
            self.client.get_table(table_ref)
        except NotFound:
            # table_ref = dataset_ref.table(table_ref)
            schema = [
                bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("count", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",  # name of column to use for partitioning
                expiration_ms=5184000000,
            )  # 90 days
            print(
                "Created table {}, partitioned on column {}".format(
                    table.table_id, table.time_partitioning.field
                )
            )

            table = self.client.create_table(table)

    def load_data(self, file_date):

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

        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_ref, table_ref))
        return job.output_rows


# if __name__ == '__main__':
#     BigQueryData()

bq = BigQueryData()

# bq.create_dataset()
# bq.create_table()
# bq.get_data()
bq.load_data('2020-06-02')
