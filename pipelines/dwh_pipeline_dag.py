from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# -------------------------
# DAG definition
# -------------------------
with DAG(
    dag_id="shopzada_dwh_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger (good for demo)
    catchup=False,
    tags=["shopzada", "dwh"],
) as dag:

    start = EmptyOperator(
        task_id="start_pipeline"
    )

    # -------------------------
    # STAGING TASKS
    # -------------------------
    stage_orders = BashOperator(
        task_id="stage_orders",
        bash_command="python /opt/airflow/transformations/stage_orders.py",
    )

    stage_order_items = BashOperator(
        task_id="stage_order_items",
        bash_command="python /opt/airflow/transformations/stage_order_items.py",
    )

    stage_customers = BashOperator(
        task_id="stage_customers",
        bash_command="python /opt/airflow/transformations/stage_customers.py",
    )

    stage_products = BashOperator(
        task_id="stage_products",
        bash_command="python /opt/airflow/transformations/stage_products.py",
    )

    stage_merchants = BashOperator(
        task_id="stage_merchants",
        bash_command="python /opt/airflow/transformations/stage_merchants.py",
    )

    stage_campaigns = BashOperator(
        task_id="stage_campaigns",
        bash_command="python /opt/airflow/transformations/stage_campaigns.py",
    )

    # -------------------------
    # WAREHOUSE & ANALYTICS
    # (SQL execution placeholder)
    # -------------------------
    build_models = EmptyOperator(
        task_id="build_dimensions_and_facts"
    )

    build_analytics = EmptyOperator(
        task_id="build_analytics_views"
    )

    end = EmptyOperator(
        task_id="end_pipeline"
    )

    # -------------------------
    # TASK DEPENDENCIES
    # -------------------------
    start >> [
        stage_orders,
        stage_order_items,
        stage_customers,
        stage_products,
        stage_merchants,
        stage_campaigns
    ]

    [
        stage_orders,
        stage_order_items,
        stage_customers,
        stage_products,
        stage_merchants,
        stage_campaigns
    ] >> build_models >> build_analytics >> end
