from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from pch_data_load import (
    get_orders_operator,
    create_order_list_in_staging_operator,
    collect_orderdetails_operator,
    collect_employees_operator,
    collect_shippers_operator,
    collect_customers_operator,
    collect_products_operator,
    build_dataset_operator,
    transfer_data_from_staging_into_dst_operator,
    clear_staging_operator)

logging.basicConfig(level=logging.INFO)

# Create DAG
with DAG(
        'pch',
        schedule='@daily',
        start_date=days_ago(0),
        catchup=False) as dag:
    get_orders = get_orders_operator(dag=dag)
    create_order_list_in_staging = create_order_list_in_staging_operator(dag=dag)
    collect_orderdetails = collect_orderdetails_operator(dag=dag)
    collect_employees = collect_employees_operator(dag=dag)
    collect_shippers = collect_shippers_operator(dag=dag)
    collect_customers = collect_customers_operator(dag=dag)
    collect_products = collect_products_operator(dag=dag)
    build_dataset = build_dataset_operator(dag=dag)
    transfer_data_from_staging_into_dst = transfer_data_from_staging_into_dst_operator(dag=dag)
    clear_staging = clear_staging_operator(dag=dag)


    get_orders >> create_order_list_in_staging >> [
        collect_employees,
        collect_shippers,
        collect_customers] >> build_dataset >> transfer_data_from_staging_into_dst >> clear_staging
