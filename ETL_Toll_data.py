# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Susmitha Kunadharaju',
    'start_date': days_ago(0),
    'email': ['ksusmi3@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL_Toll_Data',
    default_args=default_args,
    description='Toll_Data_pipeline',
    schedule_interval=timedelta(days=1),
)

# define the first task named download
download = BashOperator(
    task_id='download',
    bash_command='wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz',
    bash_command = 'tar zxvf tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging'
    dag=dag,
)

# define the second task named extract_data_from_csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," f1,2,3,4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv  > csv_data.csv',
    dag=dag,
)

# define the third task named extract_data_from_tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d"   " f5,6,7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)

# define the fourth task named extract_data_from_fixedwidth
extract_data_from_fixedwidth = BashOperator(
    task_id='extract_data_from_fixedwidth',
    bash_command='cut f7,8 /home/project/airflow/dags/finalassignment/staging/ > fixed_width_data.csv',
    dag=dag,
)

# define the fifth task named consolidate_data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data',
    dag=dag,
)

# define the six task named transform_data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" < extracted_data.csv > transformed_data.csv',
    dag=dag,
)

#Define the task pipeline

download >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixedwidth >> consolidate_data >> tranform_data



