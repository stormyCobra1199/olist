from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Args to get passed on to the python operator ------------------------------------------------------
default_args = {
    'owner': 'ming',
    'depends_on_past': False,
    'start_date': '2022-04-27',
    'email': ['208108@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# define the DAG ----------------------------------------------------------------------------------
dag = DAG(
    dag_id='odag',
    default_args=default_args,
    description='Olist DAG',
    start_date=datetime(2022,4,27),
    # At every 0,30 min of every hour
    schedule_interval='0,30 * * * *'
)

# define the init test call function --------------------------------------------------------------------
def tryinit(x):
    return x + " is a must have tool for Data Engineers."


### CREATE BQ TABLES - create tables over at Google BigQuery with pre-defined schema ---------------
def createbqtables():
  from google.cloud import bigquery
  import pandas as pd

  client = bigquery.Client(project='precise-victory-348205')

  # Set table_id to the ID of the table to create.
  # table_id = "your-project.your_dataset.your_table_name"

  table_id = 'precise-victory-348205.olist.sellers'
  schema = [
      bigquery.SchemaField("seller_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("seller_zip_code_prefix", "INTEGER", mode="REQUIRED"),
      bigquery.SchemaField("seller_city", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("seller_state", "STRING", mode="REQUIRED"),
  ]
  table = bigquery.Table(table_id, schema=schema)
  table = client.create_table(table)  # Make an API request.
  print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


  table_id = 'precise-victory-348205.olist.customers'
  schema = [
      bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("customer_unique_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("customer_zip_code_prefix", "INTEGER", mode="REQUIRED"),
      bigquery.SchemaField("customer_city", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("customer_state", "STRING", mode="REQUIRED"),
  ]
  table = bigquery.Table(table_id, schema=schema)
  table = client.create_table(table)  # Make an API request.
  print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


  table_id = 'precise-victory-348205.olist.category_name'
  schema = [
      bigquery.SchemaField("product_category_name", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("product_category_name_english", "STRING", mode="REQUIRED"),
  ]
  table = bigquery.Table(table_id, schema=schema)
  table = client.create_table(table)  # Make an API request.
  print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


  table_id = 'precise-victory-348205.olist.products'
  schema = [
      bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("product_category_name", "STRING"),
      bigquery.SchemaField("product_name_lenght", "INTEGER"),
      bigquery.SchemaField("product_description_lenght", "INTEGER"),
      bigquery.SchemaField("product_photos_qty", "INTEGER"),
      bigquery.SchemaField("product_weight_g", "INTEGER"),
      bigquery.SchemaField("product_length_cm", "INTEGER"),
      bigquery.SchemaField("product_height_cm", "INTEGER"),
      bigquery.SchemaField("product_width_cm", "INTEGER"),
  ]
  table = bigquery.Table(table_id, schema=schema)
  table = client.create_table(table)  # Make an API request.
  print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


  table_id = 'precise-victory-348205.olist.orders'
  schema = [
      bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("order_status", "STRING"),
      bigquery.SchemaField("order_purchase_timestamp", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("order_approved_at", "STRING"),
      bigquery.SchemaField("order_delivered_carrier_date", "STRING"),
      bigquery.SchemaField("order_delivered_customer_date", "STRING"),
      bigquery.SchemaField("order_estimated_delivery_date", "STRING"),    
  ]
  table = bigquery.Table(table_id, schema=schema)
  table = client.create_table(table)  # Make an API request.
  print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


  table_id = 'precise-victory-348205.olist.order_items'
  schema = [
      bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("order_item_id", "INTEGER", mode="REQUIRED"),
      bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("seller_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("shipping_limit_date", "STRING"),
      bigquery.SchemaField("price", "FLOAT"),
      bigquery.SchemaField("freight_value", "FLOAT"),
  ]
  table = bigquery.Table(table_id, schema=schema)
  table = client.create_table(table)  # Make an API request.
  print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


  table_id = 'precise-victory-348205.olist.order_payments'
  schema = [
      bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("payment_sequential", "INTEGER", mode="REQUIRED"),
      bigquery.SchemaField("payment_type", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("payment_installments", "INTEGER", mode="REQUIRED"),
      bigquery.SchemaField("payment_value", "FLOAT"),
  ]
  table = bigquery.Table(table_id, schema=schema)
  table = client.create_table(table)  # Make an API request.
  print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


  table_id = 'precise-victory-348205.olist.order_reviews'
  schema = [
      bigquery.SchemaField("review_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("review_score", "INTEGER"),
      bigquery.SchemaField("review_comment_title", "STRING"),
      bigquery.SchemaField("review_comment_message", "STRING"),
      bigquery.SchemaField("review_creation_date", "STRING"),
      bigquery.SchemaField("review_answer_timestamp", "STRING"),    
  ]
  table = bigquery.Table(table_id, schema=schema)
  table = client.create_table(table)  # Make an API request.
  print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


  table_id = 'precise-victory-348205.olist.geolocation'
  schema = [
      bigquery.SchemaField("geolocation_zip_code_prefix", "INTEGER", mode="REQUIRED"),
      bigquery.SchemaField("geolocation_lat", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("geolocation_lng", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("geolocation_city", "STRING"),
      bigquery.SchemaField("geolocation_state", "STRING"),
  ]
  table = bigquery.Table(table_id, schema=schema)
  table = client.create_table(table)  # Make an API request.
  print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
#--------------------------------------------------------------------------------------------------


### insert bq function module callable by csv2bq
def insertbq(dfname, tablename):
  from google.cloud import bigquery
  import pandas as pd
  client = bigquery.Client(project='precise-victory-348205')

  table_id = "precise-victory-348205.olist."+tablename
  print("===> Loading DF to BQ table",table_id)
  job = client.load_table_from_dataframe(dfname, table_id)  # Make an API request.
  job.result()  # Wait for the job to complete.
  table = client.get_table(table_id)  # Make an API request.
  print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))

### CSV to BQ -------------------------------------------------------------------
### load all CSV's hosted on Google Drive and store as DF 
### execute function calls to load all DF to BQ cloud data set
### (w-a-r-n-i-n-g: do not re-execute if no new data or will result in duplicates
def csv2bq():
  import pandas as pd
  # Libraries to read csv file hosted on Google Drive into Google Colab:
  #!pip install python-tds
  #!pip install -U -q PyDrive
  from pydrive.auth import GoogleAuth
  from pydrive.drive import GoogleDrive
  from google.colab import auth
  from oauth2client.client import GoogleCredentials
  # Authenticate and create the PyDrive client.
  auth.authenticate_user()
  gauth = GoogleAuth()
  gauth.credentials = GoogleCredentials.get_application_default()
  drive = GoogleDrive(gauth)

  # The shareable links from Google Drive for each CSV file hosted there
  sellers_link = '1cG5h5AW0hE7OmDZ0YPtZLVSVDnL7mCkQ' 
  category_name_link = '1Y1oiP5U1jBO0Ji5FJyRYKULw1T953obp'
  products_link = '1L8a9t3obUYIH0RzCfaCcqil7rkjgHOb-'
  order_reviews_link = '1_XXIOg3_rDSGKW61EI7MIZ0UqYYpLzfU'
  orders_link = '1PaeE67IdrWocbY5isimVwezk7EQK5pAn'
  order_payments_link = '1Tfrf7yof8pYXcK4pFuVRt28rWwW9BW4F'
  order_items_link = '1IwOvsLbP6ub9BDsXq-FWbd3d7yl1ZJvI'
  geolocation_link = '1yL1iSIwMjskL_kKza6kQH8TuI566lYnu'
  customers_link = '10cfto60JXwwIe8lewY2dao3KB9TSOuu8'

  # Read the Google Drive hosted CSVs into DF
  print("Ingesting CSV into DF...")
  #downloaded = drive.CreateFile({'id':sellers_link}) 
  #downloaded.GetContentFile('olist_sellers_dataset.csv')  
  dfsellers = pd.read_csv('olist_sellers_dataset.csv')

  # downloaded = drive.CreateFile({'id':category_name_link}) 
  # downloaded.GetContentFile('product_category_name_translation.csv')  
  dfcategoryname = pd.read_csv('product_category_name_translation.csv')

  # downloaded = drive.CreateFile({'id':products_link}) 
  # downloaded.GetContentFile('olist_products_dataset.csv')  
  dfproducts = pd.read_csv('olist_products_dataset.csv')

  # downloaded = drive.CreateFile({'id':order_reviews_link}) 
  # downloaded.GetContentFile('olist_order_reviews_dataset.csv')  
  dforderreviews = pd.read_csv('olist_order_reviews_dataset.csv')

  # downloaded = drive.CreateFile({'id':orders_link}) 
  # downloaded.GetContentFile('olist_orders_dataset.csv')  
  dforders = pd.read_csv('olist_orders_dataset.csv')

  # downloaded = drive.CreateFile({'id':order_payments_link}) 
  # downloaded.GetContentFile('olist_order_payments_dataset.csv')  
  dforderpayments = pd.read_csv('olist_order_payments_dataset.csv')

  # downloaded = drive.CreateFile({'id':order_items_link}) 
  # downloaded.GetContentFile('olist_order_items_dataset.csv')  
  dforderitems = pd.read_csv('olist_order_items_dataset.csv')

  # downloaded = drive.CreateFile({'id':geolocation_link}) 
  # downloaded.GetContentFile('olist_geolocation_dataset.csv')  
  dfgeolocation = pd.read_csv('olist_geolocation_dataset.csv')

  # downloaded = drive.CreateFile({'id':customers_link}) 
  # downloaded.GetContentFile('olist_customers_dataset.csv')  
  dfcustomers = pd.read_csv('olist_customers_dataset.csv')
  # All Datasets are now stored in Pandas Dataframes
  print("all CSV loaded in DF!\n")

  print("Now to insert into BigQuery tables...")
  insertbq(dfsellers, "sellers")               
  insertbq(dfcustomers, "customers")           
  insertbq(dfcategoryname, "category_name")    
  insertbq(dfproducts, "products")             
  insertbq(dforders, "orders")                 
  insertbq(dforderitems, "order_items")         
  insertbq(dforderpayments, "order_payments")  
  insertbq(dforderreviews, "order_reviews")
  dfgeolocation['geolocation_lat'] = dfgeolocation['geolocation_lat'].astype(str)   
                                                  # refer to notes for reason to force type to str
  dfgeolocation['geolocation_lng'] = dfgeolocation['geolocation_lng'].astype(str)   
                                                  # refer to notes for reason to force type to str
  insertbq(dfgeolocation, "geolocation")   
  print("done!\n")
#--------------------------------------------------------------------------------------------------


### DAG tasks-----------------------------------------------------------------------------------------
tryinit = PythonOperator(
    task_id='tryinit',
    python_callable=tryinit,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag)

loadpip1 = BashOperator(
    task_id='loadpip1',
    bash_command='pip install python-tds',
    dag=dag)

loadpip2 = BashOperator(
    task_id='loadpip2',
    bash_command='pip install -U -q PyDrive',
    dag=dag)

loadgc = BashOperator(
    task_id='loadgc',
    bash_command='gcloud config set project precise-victory-348205',
    dag=dag)

createbqtables = PythonOperator(
     task_id='createbqtables',
     python_callable=createbqtables,
     dag=dag)

csv2bq = PythonOperator(
    task_id='csv2bq',
    python_callable=csv2bq,
    dag=dag)
#--------------------------------------------------------------------------------------------------

# DAG execution flow---------------------------------------------------------------------------------
# tryinit >> loadpip1 >> loadpip2 >> loadgc >> createbqtables >> csv2bq
#--------------------------------------------------------------------------------------------------

