import pyspark
from pyspark.sql import SparkSession
import sys

def get_spark_session(app_name):
    try:
       spark = SparkSession.builder.master('local').appName(app_name).getOrCreate()
    except Exception as e:
        print("Error in getting a spark session")
        print("Exception: %s" % str(e))
        sys.exit(1)
    return spark

def set_log_level(spark,loglevel):
    sc = spark.sparkContext
    sc.setLogLevel(loglevel)

def conver_camel_case(string_to_convert):
    string_to_convert.title()
    converted_string = ''.join(x for x in string_to_convert.title() if not x.isspace())
    return converted_string

def convert_column_names(list_of_columns):
    temp_c = [conver_camel_case(t) for t in list_of_columns]
    converted_columns = list(zip(list_of_columns, temp_c))
    return converted_columns

def select_few_columns(df,list_of_columns):
    select_df = df.select(*list_of_columns)
    select_df.show
    return select_df

def register_temp_table(df,temp_tablename):
    df.registerTempTable(temp_tablename)

def calculate_running_total(spark,df):
    register_temp_table(df,"customer_data")
    query = """select CustomerId,
    CustomerName,
    split(CustomerName," ")[0] as CustomerFirstName,
    split(CustomerName," ")[1] as CustomerLastName,
    Segment CustomerSegment,
    Country,
    City,
    sum(case when datediff(current_date,to_timestamp(OrderDate,'dd/MM/yyyy')) <=5 then 1 else 0 end) quantityOfOrders5Days,
    sum(case when datediff(current_date,to_timestamp(OrderDate,'dd/MM/yyyy')) <=10 then 1 else 0 end) quantityOfOrders10Days,
    sum(case when datediff(current_date,to_timestamp(OrderDate,'dd/MM/yyyy')) <=30 then 1 else 0 end) quantityOfOrders30Days,
    count(*)
    from customer_data
    group by 
    CustomerId,
    CustomerName,
    CustomerFirstName,
    CustomerLastName,
    Segment,
    Country,
    City"""
    aggregated_df = spark.sql(query)
    return aggregated_df