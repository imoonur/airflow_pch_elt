from typing import Dict

# Define s3 and sql path
S3_BUCKET = 'airflow'
S3_STAGING_KEY_PREFIX = 'pch/staging/'
SQL_DIR = './dags/sql/pch/'

S3_PCH_CONN_ID = 'airflow_aws_s3_pch'
SRC_MSSQL_PCH_CONN_ID = 'airflow_mssql_pch'
SRC_POSTGRES_PCH_CONN_ID = 'airflow_postgres_pch'
SRC_POSTGRES_PCH2_CONN_ID = 'airflow_postgres_pch2'
DST_POSTGRES_PCH_CONN_ID = 'airflow_postgres_pch_dst'

final_cols = [
    'orderid',
    'orderdate',
    'shipcountry',
    'totalcost',
    'topproductcategoryname',
    'topproductsuppliercontactname',
    'shipperphone',
    'custcompanyname',
    'custcontactname',
    'custcity',
    'empfirstname',
    'empbirthdate',
    'emphiredate']


# Define SQL queries load function
def load_sql_query(filename: str, sql_dir: str = SQL_DIR) -> str:
    """Load SQL query from file and returns it."""
    with open(sql_dir + filename, 'r') as f:
        return f.read()


def get_entities() -> Dict[str, dict]:
    """Returns entities"""
    return {
        'Orders': {
            'dbms_type': 'mssql',
            'db_conn_id': SRC_MSSQL_PCH_CONN_ID,
            'create_tmp_table_query': None,
            's3_conn_id': S3_PCH_CONN_ID,
            's3_bucket': S3_BUCKET,
            'tmp_entity_list_s3_key': None,
            'tmp_table': None,
            'column_list': None,
            'get_entity_query': load_sql_query('get_orders.sql'),
            'entity_s3_key': S3_STAGING_KEY_PREFIX + 'Orders.parquet',
            'drop_tmp_table_query': None},
        'OrderList': {
            'dbms_type': None,
            'db_conn_id': None,
            'create_tmp_table_query': None,
            's3_conn_id': S3_PCH_CONN_ID,
            's3_bucket': S3_BUCKET,
            'tmp_entity_list_s3_key': None,
            'tmp_table': None,
            'column_list': None,
            'get_entity_query': None,
            'entity_s3_key': S3_STAGING_KEY_PREFIX + 'OrderList.parquet',
            'drop_tmp_table_query': None},
        'OrderDetails': {
            'dbms_type': 'mssql',
            'db_conn_id': SRC_MSSQL_PCH_CONN_ID,
            'create_tmp_table_query': load_sql_query('create_order_list_ddl_in_src.sql'),
            's3_conn_id': S3_PCH_CONN_ID,
            's3_bucket': S3_BUCKET,
            'tmp_entity_list_s3_key': S3_STAGING_KEY_PREFIX + 'OrderList.parquet',
            'tmp_table': '##OrderList',
            'column_list': ['orderid'],
            'get_entity_query': load_sql_query('get_orderdetails.sql'),
            'entity_s3_key': S3_STAGING_KEY_PREFIX + 'OrderDetails.parquet',
            'drop_tmp_table_query': None},
        'Employees': {
            'dbms_type': 'postgres',
            'db_conn_id': SRC_POSTGRES_PCH2_CONN_ID,
            'create_tmp_table_query': load_sql_query('create_employees_list_ddl_in_src.sql'),
            's3_conn_id': S3_PCH_CONN_ID,
            's3_bucket': S3_BUCKET,
            'tmp_entity_list_s3_key': S3_STAGING_KEY_PREFIX + 'Orders.parquet',
            'tmp_table': '"tmpEmployeesList"',
            'column_list': ['empid'],
            'get_entity_query': load_sql_query('get_employees.sql'),
            'entity_s3_key': S3_STAGING_KEY_PREFIX + 'Employees.parquet',
            'drop_tmp_table_query': load_sql_query('drop_employees_list.sql')},
        'Shippers': {
            'dbms_type': 'postgres',
            'db_conn_id': SRC_POSTGRES_PCH2_CONN_ID,
            'create_tmp_table_query': load_sql_query('create_shippers_list_ddl_in_src.sql'),
            's3_conn_id': S3_PCH_CONN_ID,
            's3_bucket': S3_BUCKET,
            'tmp_entity_list_s3_key': S3_STAGING_KEY_PREFIX + 'Orders.parquet',
            'tmp_table': '"tmpShippersList"',
            'column_list': ['shipperid'],
            'get_entity_query': load_sql_query('get_shippers.sql'),
            'entity_s3_key': S3_STAGING_KEY_PREFIX + 'Shippers.parquet',
            'drop_tmp_table_query': load_sql_query('drop_shippers_list.sql')},
        'Customers': {
            'dbms_type': 'postgres',
            'db_conn_id': SRC_POSTGRES_PCH_CONN_ID,
            'create_tmp_table_query': load_sql_query('create_customers_list_ddl_in_src.sql'),
            's3_conn_id': S3_PCH_CONN_ID,
            's3_bucket': S3_BUCKET,
            'tmp_entity_list_s3_key': S3_STAGING_KEY_PREFIX + 'Orders.parquet',
            'tmp_table': '"tmpCustomersList"',
            'column_list': ['custid'],
            'get_entity_query': load_sql_query('get_customers.sql'),
            'entity_s3_key': S3_STAGING_KEY_PREFIX + 'Customers.parquet',
            'drop_tmp_table_query': load_sql_query('drop_customers_list.sql')},
        'Products': {
            'dbms_type': 'postgres',
            'db_conn_id': SRC_POSTGRES_PCH_CONN_ID,
            'create_tmp_table_query': load_sql_query('create_products_list_ddl_in_src.sql'),
            's3_conn_id': S3_PCH_CONN_ID,
            's3_bucket': S3_BUCKET,
            'tmp_entity_list_s3_key': S3_STAGING_KEY_PREFIX + 'OrderDetails.parquet',
            'tmp_table': '"tmpProductsList"',
            'column_list': ['productid'],
            'get_entity_query': load_sql_query('get_products.sql'),
            'entity_s3_key': S3_STAGING_KEY_PREFIX + 'Products.parquet',
            'drop_tmp_table_query': load_sql_query('drop_products_list.sql')},
        'Data': {
            'dbms_type': None,
            'db_conn_id': DST_POSTGRES_PCH_CONN_ID,
            'create_tmp_table_query': load_sql_query('create_data_ddl_in_dst.sql'),
            's3_conn_id': S3_PCH_CONN_ID,
            's3_bucket': S3_BUCKET,
            'tmp_entity_list_s3_key': None,
            'tmp_table': '"Data"',
            'column_list': final_cols,
            'get_entity_query': None,
            'entity_s3_key': S3_STAGING_KEY_PREFIX + 'Data.parquet',
            'drop_tmp_table_query': None}}


