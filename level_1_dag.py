# IMPORT ALL THE MODULES, LIBRARIES
import airflow
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime,timedelta

#VARIABLE SECTION
PROJECT_ID = "brave-inn-457105-e9"
DATASET_NAME_1 = "raw_ds"
DATASET_NAME_2 = "insight_ds"
TABLE_NAME_1 = "emp_raw"
TABLE_NAME_2 = "dep_raw"
TABLE_NAME_3 = "empdep_in"
LOCATION = "us-central1"
INSERT_ROWS_QUERY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` AS
SELECT
  e.EmployeeID,
  CONCAT(e.FirstName," ",e.LastName) AS FullName,
  e.Email,
  e.Salary,
  e.JoinDate,
  d.DepartmentID,
  d.DepartmentName
FROM
  `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` AS e
LEFT JOIN
  `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` AS d
ON
  e.DepartmentID = d.DepartmentID;"""

args = {
    'owner': 'Nagarjuna',
    'start_date': datetime(2025,5,3),
    'retries':2,
    'retry_delay': timedelta(minutes=5)
}

#DEFINE THE DAG
with DAG(
    "LEVEL_1_DAG",
    schedule_interval = "30 17 * * *",
    default_args = args,
    description = "gcs to bigquery job"
) as DAG:    

#DEFINE THE TASKS

    task_1 = GCSToBigQueryOperator(
        task_id = "employee_example",
        bucket = "brave-inn-457105-e9-test-bucket",
        source_objects = ["employee.csv"],
        destination_project_dataset_table = f"{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields = [
            {"name": "EmployeeID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Salary", "type": "STRING", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition = "WRITE_TRUNCATE",
    )

    task_2 = GCSToBigQueryOperator(
        task_id = "deprt_example",
        bucket = "brave-inn-457105-e9-test-bucket",
        source_objects = ["departments.csv"],
        destination_project_dataset_table = f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields = [
            {"name": "DepartmentID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition = "WRITE_TRUNCATE",
    )

    task_3 = BigQueryInsertJobOperator(
        task_id = "empDep_exmaple",
        configuration = {
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
        location=LOCATION,
    )

#DEFINE THE DEPENDENCY
(task_1,task_2) >> task_3
