from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator, S3DeleteObjectsOperator
from io import BytesIO
from pandas import DataFrame, read_parquet
import logging
from enum import Enum
from typing import List

from pch_entities import S3_PCH_CONN_ID, S3_BUCKET, S3_STAGING_KEY_PREFIX, get_entities

logging.basicConfig(level=logging.INFO)


def df_from_s3_parquet(s3_hook: S3Hook, bucket: str, key: str) -> DataFrame:
    """Returns pandas dataframe from s3 parquet object."""
    obj = s3_hook.get_key(
        key=key,
        bucket_name=bucket)
    with BytesIO() as parquet_buffer:
        obj.download_fileobj(parquet_buffer)
        df = read_parquet(parquet_buffer)
    return df


def df_to_s3_parquet(df: DataFrame, s3_hook: S3Hook, bucket: str, key: str) -> None:
    """Upload pandas dataframe to s3 parquet object."""
    with BytesIO() as parquet_buffer:
        df.to_parquet(parquet_buffer)
        s3_hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=key,
            bucket_name=bucket,
            replace=True)


# Define enumerated data types
DBMSType = Enum('DBMSType', ['mssql', 'postgres'])


def collect_entity_data(
        dbms_type: DBMSType,
        db_conn_id: str,
        create_tmp_table_query: str,
        s3_conn_id: str,
        s3_bucket: str,
        tmp_entity_list_s3_key: str,
        tmp_table: str,
        column_list: List[str],
        get_entity_query: str,
        entity_s3_key: str,
        drop_tmp_table_query: str) -> None:
    """
    Collect entity data from DBMS using temp table and staging.

    :param dbms_type: using dbms type ['mssql', 'postgres']
    :param db_conn_id: reference to a specific SQL database connection id
    :param create_tmp_table_query: temporary table creation SQL query
    :param s3_conn_id: reference to a specific S3 connection id
    :param s3_bucket: reference to a specific S3 bucket
    :param tmp_entity_list_s3_key: s3 key entity list using for filtering on source side
    :param tmp_table: DB temporary table name
    :param column_list: list of column names to use in the insert SQL
    :param get_entity_query: entity retrieving SQL query
    :param entity_s3_key: entity S3 key
    :param drop_tmp_table_query: temporary table drop SQL query
    """
    if dbms_type not in [e.name for e in DBMSType]:
        raise ValueError('dbms_type must be in {}'.format([e.name for e in DBMSType]))
    elif dbms_type == 'mssql':
        db_hook = MsSqlHook(mssql_conn_id=db_conn_id)
    else:
        db_hook = PostgresHook(postgres_conn_id=db_conn_id)
    with db_hook.get_conn() as conn:
        with conn.cursor() as cur:
            # create temp table on the src side
            logging.info('Create temporary table in {} source ...'.format(db_conn_id))
            cur.execute(create_tmp_table_query)
            conn.commit()
            logging.info('Temporary table created successfully.')
            s3_hook = S3Hook(aws_conn_id=s3_conn_id)
            # get list from s3
            logging.info('Download {} from {} S3 bucket and create temporary entity list ...'.format(
                tmp_entity_list_s3_key, s3_bucket))
            tmp_entity_list = df_from_s3_parquet(
                s3_hook, s3_bucket, tmp_entity_list_s3_key)[column_list].drop_duplicates()
            logging.info('Download completed successfully. List shape: {}.'.format(tmp_entity_list.shape))
            # fill temp table on the src side
            logging.info('Fill temporary table {} in {} source ...'.format(tmp_table, db_conn_id))
            rows = list(tmp_entity_list.itertuples(index=False, name=None))
            db_hook.insert_rows(table=tmp_table, rows=rows, target_fields=column_list)
            logging.info('Temporary table filled successfully. {} rows was written.'.format(len(list(rows))))
            # get table from src
            logging.info('Read entity table from {} source ...'.format(db_conn_id))
            entity = db_hook.get_pandas_df(sql=get_entity_query)
            logging.info('Entity table was read successfully. Entity shape: {}.'.format(entity.shape))
            logging.info('Drop temporary table from {} source ...'.format(db_conn_id))
            if dbms_type == 'postgres':
                # drop temp table
                cur.execute(drop_tmp_table_query)
                conn.commit()
            logging.info('Temporary table was dropped successfully.')
            # put table into s3
            logging.info('Upload entity {} into {} S3 bucket ...'.format(entity_s3_key, s3_bucket))
            df_to_s3_parquet(entity, s3_hook, s3_bucket, entity_s3_key)
            logging.info('Entity uploaded successfully.')


def create_entity_list_in_s3(
        s3_conn_id: str,
        entity_s3_bucket: str,
        entity_s3_key: str,
        list_s3_bucket: str,
        list_s3_key: str,
        column_list: List[str]) -> None:
    """Load entity from S3, create entity list and put them into S3."""
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    # Download parquet data from s3
    logging.info('Download entity {} from {} S3 bucket ...'.format(entity_s3_key, entity_s3_bucket))
    entity = df_from_s3_parquet(s3_hook, entity_s3_bucket, entity_s3_key)
    logging.info('Download completed successfully. Entity shape: {}.'.format(entity.shape))
    # Upload parquet data to s3
    logging.info('Upload entity into {} S3 bucket as {} ...'.format(list_s3_bucket, list_s3_key))
    df_to_s3_parquet(entity[column_list], s3_hook, list_s3_bucket, list_s3_key)
    logging.info('Upload completed successfully.')


# Get entities for dag operators making
entities = get_entities()


def pre_execute_log(context):
    logging.info('Running task {} ...'.format(context['task'].task_id))


def post_execute_log(context, result):
    logging.info('Task {} completed successfully.'.format(context['task'].task_id))


def get_orders_operator(dag: DAG) -> SqlToS3Operator:
    """Make operator for get orders task"""
    return SqlToS3Operator(
        task_id='get_orders',
        sql_conn_id=entities['Orders']['db_conn_id'],
        query=entities['Orders']['get_entity_query'],
        aws_conn_id=entities['Orders']['s3_conn_id'],
        s3_bucket=entities['Orders']['s3_bucket'],
        s3_key=entities['Orders']['entity_s3_key'],
        file_format='parquet',
        replace=True,
        pre_execute=pre_execute_log,
        post_execute=post_execute_log,
        dag=dag)


def create_order_list_in_staging_operator(dag: DAG) -> PythonOperator:
    """Make operator for create order list in staging task"""
    return PythonOperator(
        task_id="create_order_list_in_staging",
        python_callable=create_entity_list_in_s3,
        op_kwargs={
            's3_conn_id': entities['OrderList']['s3_conn_id'],
            'entity_s3_bucket': entities['Orders']['s3_bucket'],
            'entity_s3_key': entities['Orders']['entity_s3_key'],
            'list_s3_bucket': entities['OrderList']['s3_bucket'],
            'list_s3_key': entities['OrderList']['entity_s3_key'],
            'column_list': ['orderid']},
        dag=dag)


def collect_orderdetails_operator(dag: DAG) -> PythonOperator:
    """Make operator for collect orderdetails task"""
    return PythonOperator(
        task_id="collect_orderdetails",
        python_callable=collect_entity_data,
        op_kwargs={**entities['OrderDetails']},
        dag=dag)


def collect_employees_operator(dag: DAG) -> PythonOperator:
    """Make operator for collect employees task"""
    return PythonOperator(
        task_id="collect_employees",
        python_callable=collect_entity_data,
        op_kwargs={**entities['Employees']},
        dag=dag)


def collect_shippers_operator(dag: DAG) -> PythonOperator:
    """Make operator for collect shippers task"""
    return PythonOperator(
        task_id="collect_shippers",
        python_callable=collect_entity_data,
        op_kwargs={**entities['Shippers']},
        dag=dag)


def collect_customers_operator(dag: DAG) -> PythonOperator:
    """Make operator for collect customers task"""
    return PythonOperator(
        task_id="collect_customers",
        python_callable=collect_entity_data,
        op_kwargs={**entities['Customers']},
        dag=dag)


def collect_products_operator(dag: DAG) -> PythonOperator:
    """Make operator for collect products task"""
    return PythonOperator(
        task_id="collect_products",
        python_callable=collect_entity_data,
        op_kwargs={**entities['Products']},
        dag=dag)


def build_dataset():
    """Build dataset and upload it into S3 storage"""
    s3_hook = S3Hook(aws_conn_id=entities['Data']['s3_conn_id'])
    # get entities from s3
    s3_bucket = entities['Data']['s3_bucket']
    logging.info('Download entities from {} S3 bucket ...'.format(s3_bucket))
    orders = df_from_s3_parquet(s3_hook, s3_bucket, entities['Orders']['entity_s3_key'])
    orderdetails = df_from_s3_parquet(s3_hook, s3_bucket, entities['OrderDetails']['entity_s3_key'])
    employees = df_from_s3_parquet(s3_hook, s3_bucket, entities['Employees']['entity_s3_key'])
    shippers = df_from_s3_parquet(s3_hook, s3_bucket, entities['Shippers']['entity_s3_key'])
    customers = df_from_s3_parquet(s3_hook, s3_bucket, entities['Customers']['entity_s3_key'])
    products = df_from_s3_parquet(s3_hook, s3_bucket, entities['Products']['entity_s3_key'])
    logging.info('Download completed successfully.')
    # prepare data
    logging.info('Prepare data ...')
    orderdetails['cost'] = orderdetails['unitprice'] * orderdetails['qty'] * (1 - orderdetails['discount'])
    order_totalcost = orderdetails.groupby('orderid')['cost'].sum().rename('totalcost').to_frame()
    orders = orders.merge(order_totalcost, how='inner', left_on='orderid', right_index=True)
    order_topproductid = orderdetails.groupby('orderid')['productid'].agg(lambda s: s.mode().iloc[0]).rename(
        'topproductid').to_frame().reset_index()
    order_topproduct = order_topproductid.merge(
        products,
        how='inner',
        left_on='topproductid', right_on='productid')[['orderid', 'categoryname', 'suppliercontactname']]
    orders = orders.merge(order_topproduct, how='left', on='orderid').rename(
        columns={
            'categoryname': 'topproductcategoryname',
            'suppliercontactname': 'topproductsuppliercontactname'})

    orders = orders.merge(shippers, how='left', on='shipperid').rename(columns={'phone': 'shipperphone'})
    orders = orders.merge(customers, how='left', on='custid').rename(
        columns={
            'companyname': 'custcompanyname',
            'contactname': 'custcontactname',
            'city': 'custcity'})
    orders = orders.merge(employees, how='left', on='empid').rename(
        columns={
            'firstname': 'empfirstname',
            'birthdate': 'empbirthdate',
            'hiredate': 'emphiredate'})

    orders = orders[entities['Data']['column_list']]
    logging.info('Data prepared successfully. Shape: {}.'.format(orders.shape))
    # put table into s3
    logging.info('Upload prepared data as {} into {} S3 bucket ...'.format(
        entities['Data']['entity_s3_key'], s3_bucket))
    df_to_s3_parquet(orders, s3_hook, s3_bucket, entities['Data']['entity_s3_key'])
    logging.info('Prepared data uploaded successfully.')


def build_dataset_operator(dag: DAG) -> PythonOperator:
    """Make operator for build dataset task"""
    return PythonOperator(
        task_id="build_dataset",
        python_callable=build_dataset,
        dag=dag)


def transfer_data_from_staging_into_dst(
        db_conn_id: str,
        tmp_table: str,
        s3_conn_id: str,
        s3_bucket: str,
        entity_s3_key: str,
        column_list: str) -> None:
    """Transfer data from S3 into DB"""
    postgres_hook = PostgresHook(postgres_conn_id=db_conn_id)
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            # create table on the dst side
            logging.info('Create table on the {} destination ...'.format(db_conn_id))
            cur.execute(entities['Data']['create_tmp_table_query'])
            conn.commit()
            logging.info('Table created successfully.')
            # get data from s3
            logging.info('Download {} from {} S3 bucket ...'.format(entity_s3_key, s3_bucket))
            s3_hook = S3Hook(aws_conn_id=s3_conn_id)
            data = df_from_s3_parquet(s3_hook, s3_bucket, entity_s3_key)
            logging.info('Download completed successfully.')
            # fill data table on the dst side
            logging.info('Fill {} table on the {} destination ...'.format(tmp_table, db_conn_id))
            rows = list(data.itertuples(index=False, name=None))
            postgres_hook.insert_rows(table=tmp_table, rows=rows, target_fields=column_list)
            logging.info('Destination table filled successfully. {} rows was written.'.format(len(list(rows))))


def transfer_data_from_staging_into_dst_operator(dag: DAG) -> PythonOperator:
    """Make operator for transfer data from staging into dst task"""
    return PythonOperator(
        task_id="transfer_data_from_staging_into_dst",
        python_callable=transfer_data_from_staging_into_dst,
        op_kwargs={
            'db_conn_id': entities['Data']['db_conn_id'],
            'tmp_table': entities['Data']['tmp_table'],
            's3_conn_id': entities['Data']['s3_conn_id'],
            's3_bucket': entities['Data']['s3_bucket'],
            'entity_s3_key': entities['Data']['entity_s3_key'],
            'column_list': entities['Data']['column_list']},
        dag=dag)


def clear_staging_operator(dag: DAG) -> S3DeleteObjectsOperator:
    """Make operator for remove all objects in staging"""
    return S3DeleteObjectsOperator(
        task_id="clear_staging",
        aws_conn_id=S3_PCH_CONN_ID,
        bucket=S3_BUCKET,
        prefix=S3_STAGING_KEY_PREFIX,
        pre_execute=pre_execute_log,
        post_execute=post_execute_log,
        dag=dag)
