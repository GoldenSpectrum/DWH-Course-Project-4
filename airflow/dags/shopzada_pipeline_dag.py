import sys
import os
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# ================= INGESTION IMPORTS =================
from ingestion.business_ingest import ingest_product_list
from ingestion.customer_ingest import (
    ingest_user_job,
    ingest_user_data,
    ingest_user_credit_card
)
from ingestion.enterprise_ingest import (
    ingest_merchant_data,
    ingest_staff_data,
    ingest_order_with_merchant
)
from ingestion.marketing_ingest import (
    ingest_campaign_data,
    ingest_transactional_campaign_data
)
from ingestion.ops_ingest import (
    ingest_line_item_prices,
    ingest_line_item_products,
    ingest_order_data,
    ingest_order_delays
)

# ================= TRANSFORM IMPORTS =================
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
    dag_id="shopzada_full_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="End-to-end ShopZada ingestion + transform pipeline",
    template_searchpath="/opt/airflow", 
) as dag:

    # ==================================================
    # INGESTION TASKS
    # ==================================================
    ingest_product_list_task = PythonOperator(
        task_id="ingest_product_list",
        python_callable=ingest_product_list
    )

    ingest_tasks = [
        PythonOperator(task_id="ingest_user_job", python_callable=ingest_user_job),
        PythonOperator(task_id="ingest_user_data", python_callable=ingest_user_data),
        PythonOperator(task_id="ingest_user_credit_card", python_callable=ingest_user_credit_card),
        PythonOperator(task_id="ingest_merchant_data", python_callable=ingest_merchant_data),
        PythonOperator(task_id="ingest_staff_data", python_callable=ingest_staff_data),
        PythonOperator(task_id="ingest_order_with_merchant", python_callable=ingest_order_with_merchant),
        PythonOperator(task_id="ingest_campaign_data", python_callable=ingest_campaign_data),
        PythonOperator(task_id="ingest_transactional_campaign_data", python_callable=ingest_transactional_campaign_data),
        PythonOperator(task_id="ingest_line_item_prices", python_callable=ingest_line_item_prices),
        PythonOperator(task_id="ingest_line_item_products", python_callable=ingest_line_item_products),
        PythonOperator(task_id="ingest_order_data", python_callable=ingest_order_data),
        PythonOperator(task_id="ingest_order_delays", python_callable=ingest_order_delays),
    ]

    finish_ingestion = EmptyOperator(task_id="finish_ingestion")

    # ==================================================
    # DIMENSION TRANSFORMS
    # ==================================================
    dim_tasks = [
        PythonOperator(task_id="transform_dim_user", python_callable=transform_dim_user),
        PythonOperator(task_id="transform_dim_merchant", python_callable=transform_dim_merchant),
        PythonOperator(task_id="transform_dim_staff", python_callable=transform_dim_staff),
        PythonOperator(task_id="transform_dim_product", python_callable=transform_dim_product),
        PythonOperator(task_id="transform_dim_campaign", python_callable=transform_dim_campaign),
        PythonOperator(task_id="transform_dim_date", python_callable=transform_dim_date),
    ]

    # ==================================================
    # FACT TRANSFORMS
    # ==================================================
    fact_orders = PythonOperator(
        task_id="transform_fact_orders",
        python_callable=transform_fact_orders
    )

    fact_line_items = PythonOperator(
        task_id="transform_fact_line_items",
        python_callable=transform_fact_line_items
    )

    fact_campaign = PythonOperator(
        task_id="transform_fact_campaign",
        python_callable=transform_fact_campaign_transaction
    )

    fact_delays = PythonOperator(
        task_id="transform_fact_delays",
        python_callable=transform_fact_order_delays
    )
    
    finish_facts = EmptyOperator(task_id="finish_facts")

    create_order_analytics_views = PostgresOperator(
        task_id="create_order_analytics_views",
        postgres_conn_id="shopzada_postgres",
        sql="analytics/order_analytics.sql"
    )

    create_line_item_analytics_views = PostgresOperator(
        task_id="create_line_item_analytics_views",
        postgres_conn_id="shopzada_postgres",
        sql="analytics/line_item_analytics.sql"
    )

    create_campaign_analytics_views = PostgresOperator(
        task_id="create_campaign_analytics_views",
        postgres_conn_id="shopzada_postgres",
        sql="analytics/campaign_analytics.sql"
    )

    create_delivery_analytics_views = PostgresOperator(
        task_id="create_delivery_analytics_views",
        postgres_conn_id="shopzada_postgres",
        sql="analytics/delivery_performance_analytics.sql"
    )

    # All facts → Analytics views
    fact_tasks = [
        fact_line_items,
        fact_campaign,
        fact_delays,
    ]

    analytics_tasks = [
        create_order_analytics_views,
        create_line_item_analytics_views,
        create_campaign_analytics_views,
        create_delivery_analytics_views,
    ]


    # ==================================================
    # DEPENDENCIES
    # ==================================================
    ingest_product_list_task >> ingest_tasks >> finish_ingestion
    finish_ingestion >> dim_tasks

    # Dimensions → Facts
    dim_tasks >> fact_orders
    fact_orders >> fact_tasks >> finish_facts
    finish_facts >> analytics_tasks




