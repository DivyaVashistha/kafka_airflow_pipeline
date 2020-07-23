## Introduction
```
Implementation of a pipeline with 3 tasks and logging them

using:
apache airflow
apache kafka
python

```
## Download

```
Download the folder into your local system
```


Activate your Python virtual environment, and download the required libraries
```
source venv/bin/activate
export PYTHONPATH = '.'
pip install -r requirements.txt
```

## How to run the pipeline on local system

Follow the guide to ensure a smooth run via a terminal
```
Open terminal (Let 1):
    1.  export AIRFLOW_HOME='pwd' airflow_home
    2.  cd airflow_home
    3.  airflow webserver
Open terminal (Let 2):
    1.  export AIRFLOW_HOME='pwd' airflow_home
    2.  cd airflow_home
    3.  airflow scheduler
```

BigQuery table has the following schema:
```
state	STRING	REQUIRED	
count	INTEGER	REQUIRED	
date	DATE	REQUIRED	

The table is partitioned on date.	
```

Key Entities in Code
----
```   
+-- airflow_home
|  +--pwd
        |   +--dags
        |   +--logs
|  +--my_pipeline
        |   +--reports
        |   +--logs.csv
        |   +--my Project.json
```
Credentials
----
``` 
Please add the json to your bigquery project in `airflow_home/my_pipeline`
``` 

Screenshots
----
Screen Capture of bigquery table,airflow webserver are in the folder `screenshots`


