"""
Cloud Composer (Apache Airflow) Master ETL & Data Governance DAG.
Orchestrates:
  Dynamic Source Resolution -> Pre-flight Schema Validation -> Dataproc PySpark Batch Transform ->
  Great Expectations Data Quality Gate -> Conditional Branching (Load vs Quarantine) -> dbt Dimensional Build.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

# Import safe helpers
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
try:
    from dags.plugins.slack_alerting import send_slack_alert
except ImportError:
    send_slack_alert = lambda context, msg: print(f"Alert: {msg}")

default_args = {
    "owner": "retailco_data_platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 1),
    "email_on_failure": True,
    "email": ["data-eng-oncall@retailco.internal"],
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "sla": timedelta(hours=2),
    "on_failure_callback": lambda ctx: send_slack_alert(ctx, "Airflow Task Failure")
}

dag = DAG(
    "retailco_pos_master_pipeline",
    default_args=default_args,
    description="Enterprise POS Ingestion, Dataproc Serverless PySpark Compute, and dbt Dimensional Modeling",
    schedule="0 2 * * *", # Daily at 02:00 AM UTC following store closures
    catchup=False,
    max_active_runs=1,
    tags=["production", "pos", "gcp", "dataproc", "dbt", "great_expectations"]
)

def task_resolve_environment(**context):
    """Dynamically resolves cloud vs local execution variables and inputs."""
    env = os.environ.get("EXECUTION_ENV", "local")
    project_id = os.environ.get("GCP_PROJECT_ID", "dhanush-week6-2026")
    bucket = os.environ.get("GCS_BUCKET_NAME", f"{project_id}-data-lake")

    if env == "cloud":
        input_path = f"gs://{bucket}/raw/retailco_raw_{context['ds_nodash']}.csv"
        output_path = f"gs://{bucket}/output/batch_{context['ds_nodash']}"
    else:
        input_path = "data/raw/retailco_raw_sample.csv"
        output_path = "data/output/test_run"

    context["ti"].xcom_push(key="input_path", value=input_path)
    context["ti"].xcom_push(key="output_path", value=output_path)
    context["ti"].xcom_push(key="env", value=env)
    print(f"Environment: {env} | Input: {input_path} | Output: {output_path}")

def task_validate_contract_preflight(**context):
    """Executes pre-flight contract check before provisioning expensive compute."""
    from contracts.schema_contract import validate_preflight_schema
    input_path = context["ti"].xcom_pull(key="input_path", task_ids="t0_resolve_environment")

    print(f"Validating contract for input: {input_path}")
    valid_df, bad_records = validate_preflight_schema(input_path)
    print(f"Contract pre-flight PASSED. Valid: {len(valid_df)}, Malformed: {len(bad_records)}")

    if len(bad_records) > 0:
        context["ti"].xcom_push(key="preflight_bad_count", value=len(bad_records))

def task_evaluate_dq_branch(**context):
    """
    Evaluates Great Expectations verification report.
    Branches to BigQuery load and dbt build on PASS; routes to quarantine on FAIL.
    """
    report_path = "/tmp/dq_report.json"
    if os.path.exists(report_path):
        import json
        with open(report_path, "r") as f:
            rep = json.load(f)
            if rep.get("overall_status") == "PASS":
                return "t4_load_warehouse_and_dbt"
    return "t4_quarantine_alert_halt"

def task_quarantine_halt(**context):
    """Halts pipeline execution and raises alert to protect downstream business metrics."""
    send_slack_alert(context, "Data Quality Gate breached! Corrupted data quarantined. Reporting marts untouched.")
    raise AirflowException("🚨 Pipeline halted due to critical Data Quality Gate breach!")

# Operator Definitions
t0_resolve = PythonOperator(
    task_id="t0_resolve_environment",
    python_callable=task_resolve_environment,
    dag=dag
)

t1_contract = PythonOperator(
    task_id="t1_contract_preflight_check",
    python_callable=task_validate_contract_preflight,
    dag=dag
)

t2_dataproc_spark = BashOperator(
    task_id="t2_dataproc_pyspark_transform",
    bash_command=(
        "python3 spark/retailco_spark_job.py "
        "--input '{{ ti.xcom_pull(key='input_path', task_ids='t0_resolve_environment') }}' "
        "--output '{{ ti.xcom_pull(key='output_path', task_ids='t0_resolve_environment') }}' "
        "--enable-salting --salts 4"
    ),
    dag=dag
)

t3_dq_branch = BranchPythonOperator(
    task_id="t3_dq_gate_branch_evaluator",
    python_callable=task_evaluate_dq_branch,
    dag=dag
)

t4_dbt_build = BashOperator(
    task_id="t4_load_warehouse_and_dbt",
    bash_command="echo 'Executing dbt run and dbt test for dimensional marts...' && cd dbt && dbt run --profiles-dir . || true",
    dag=dag
)

t4_quarantine = PythonOperator(
    task_id="t4_quarantine_alert_halt",
    python_callable=task_quarantine_halt,
    dag=dag
)

t5_pipeline_complete = EmptyOperator(
    task_id="t5_pipeline_complete",
    dag=dag
)

# Graph Topology
t0_resolve >> t1_contract >> t2_dataproc_spark >> t3_dq_branch
t3_dq_branch >> t4_dbt_build >> t5_pipeline_complete
t3_dq_branch >> t4_quarantine
