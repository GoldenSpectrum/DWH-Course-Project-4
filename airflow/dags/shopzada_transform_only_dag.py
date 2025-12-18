from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ================= TRANSFORMS =================
from transform.dim_user_transform import transform_dim_user
from transform.dim_merchant_transform import transform_dim_merchant
from transform.dim_staff_transform import transform_dim_staff
from transform.dim_product_transform import transform_dim_product
from transform.dim_campaign_transform import transform_dim_campaign
from transform.dim_date_transform import transform_dim_date

from transform.fact_orders_transform import transform_fact_orders
from transform.fact_line_items_transform import transform_fact_line_items
from transform.fact_campaign_transaction_transform import transform_fact_campaign_transaction
from transform.fact_order_delays_transform import transform_fact_order_delays


default_args = {
    "owner": "shopzada_data_team",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="shopzada_transform_only",
    default_args=default_args,
    schedule_interval=None,   # 👈 MANUAL ONLY
    catchup=False,
    description="Transform-only DAG for ShopZada (dims + facts)",
) as dag:

    # ================= DIMENSIONS =================
    dim_tasks = [
        PythonOperator(task_id="transform_dim_user", python_callable=transform_dim_user),
        PythonOperator(task_id="transform_dim_merchant", python_callable=transform_dim_merchant),
        PythonOperator(task_id="transform_dim_staff", python_callable=transform_dim_staff),
        PythonOperator(task_id="transform_dim_product", python_callable=transform_dim_product),
        PythonOperator(task_id="transform_dim_campaign", python_callable=transform_dim_campaign),
        PythonOperator(task_id="transform_dim_date", python_callable=transform_dim_date),
    ]

    # ================= FACTS =================
    fact_orders = PythonOperator(
        task_id="transform_fact_orders",
        python_callable=transform_fact_orders
    )

    fact_tasks = [
        PythonOperator(task_id="transform_fact_line_items", python_callable=transform_fact_line_items),
        PythonOperator(task_id="transform_fact_campaign", python_callable=transform_fact_campaign_transaction),
        PythonOperator(task_id="transform_fact_delays", python_callable=transform_fact_order_delays),
    ]

    finish_facts = EmptyOperator(task_id="finish_facts")

    # ================= ANALYTICS (OPTIONAL) =================
    analytics_tasks = [
        PostgresOperator(
            task_id="create_order_analytics_views",
            postgres_conn_id="shopzada_postgres",
            sql="analytics/order_analytics.sql"
        ),
        PostgresOperator(
            task_id="create_line_item_analytics_views",
            postgres_conn_id="shopzada_postgres",
            sql="analytics/line_item_analytics.sql"
        ),
        PostgresOperator(
            task_id="create_campaign_analytics_views",
            postgres_conn_id="shopzada_postgres",
            sql="analytics/campaign_analytics.sql"
        ),
        PostgresOperator(
            task_id="create_delivery_analytics_views",
            postgres_conn_id="shopzada_postgres",
            sql="analytics/delivery_performance_analytics.sql"
        ),
    ]

    # ================= DEPENDENCIES =================
    dim_tasks >> fact_orders
    fact_orders >> fact_tasks >> finish_facts
    finish_facts >> analytics_tasks
