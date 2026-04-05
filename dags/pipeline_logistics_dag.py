from airflow import DAG
from airflow.sensors.bash import BashSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Dfinition des arguments par dfaut du DAG
default_args = {
    'owner': 'admin_logistics',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Cration du DAG
with DAG(
    'logistics_medallion_pipeline_dag',
    default_args=default_args,
    description='Orchestration Airflow de la pipeline logistique (S3 -> dbt)',
    schedule_interval=timedelta(minutes=15),
    catchup=False
) as dag:

    # 1. SENSOR : Attente de l'arrive de donne dans MinIO (Couche Bronze)
    # L'image MinIO client doit tre disponible ou utilise via HTTP
    wait_for_bronze_data = BashSensor(
        task_id='wait_for_bronze_s3',
        bash_command='curl -s -f -I http://minio:9000/logistics-bronze/raw_orders/ | grep "200 OK" || exit 0',
        # On utilise une requete HTTP simple en guise de Sensor S3 sans alourdir la dpendance boto3
        poke_interval=10,
        timeout=600
    )

    # 2. RUN DBT : Transformation Silver vers Gold via dbt
    # Attention, le run s'excute dans le conteneur Airflow. Il faut que dbt-postgres soit install.
    # Dans une configuration de Prod, on utiliserait le DbtRunOperator (Cosmos).
    run_dbt_models = BashOperator(
        task_id='dbt_run_gold_layer',
        bash_command='cd /opt/logistics/dbt_logistics && dbt run --profiles-dir .'
    )

    # 3. TEST DBT : Excution des dbt-expectations
    test_dbt_models = BashOperator(
        task_id='dbt_test_expectations',
        bash_command='cd /opt/logistics/dbt_logistics && dbt test --profiles-dir .'
    )

    # Workflow dfini par ordre d'excution
    wait_for_bronze_data >> run_dbt_models >> test_dbt_models
