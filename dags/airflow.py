import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="ittf_scores",
    default_args={
        "owner": "Anja Szczepanik",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start_job = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

ranking_analysis_job = SparkSubmitOperator(
    task_id="ranking_analysis_job",
    conn_id="spark-conn",
    application="target/scala-2.12/ittf-scores-assembly-0.1.0-SNAPSHOT.jar",
    dag=dag
)

end_job = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start_job >> ranking_analysis_job >> end_job
