from pyspark.sql import functions as F
from pyspark.sql.functions import year, to_timestamp, month, dayofmonth

from models import sales_dataset_columns, customer_dataset_columns
from read import read_files
from util import get_spark_session, convert_column_names, select_few_columns, calculate_running_total, set_log_level
from write import output_to_files


def main():
    # os.environ['HADOOP_HOME'] = 'C:\hadoop'
    # input_data_dir = os.environ.get("INPUT_DIR")
    input_data_dir = "data/landing"
    spark = get_spark_session("Retail Superstore")
    set_log_level(spark, "info")

    logger = spark._jvm.org.apache.log4j.LogManager \
        .getLogger("log4j.logger.org.apache.hadoop.fs.s3a.S3AStorageStatistics")

    logger.info("Reading input data from landing file")

    #  Reading input data
    df = read_files(spark, input_data_dir, "*", "csv")

    # column name changes and standardization of columns
    try:
        standardized_columns = convert_column_names(df.columns)
        standardized_df = df.select([F.col(x).alias(y) for (x, y) in standardized_columns])

        df_with_partitioned_columns = standardized_df. \
            withColumn("OrderYear", year(to_timestamp(standardized_df.OrderDate, 'dd/MM/yyyy'))). \
            withColumn("OrderMonth", month(to_timestamp(standardized_df.OrderDate, 'dd/MM/yyyy'))). \
            withColumn("OrderDay", dayofmonth(to_timestamp(standardized_df.OrderDate, 'dd/MM/yyyy')))
        # write data to raw layer

        output_to_files("data/raw/", 'csv', df_with_partitioned_columns, "append", "yes")
        logger.info("Output has been written to raw layer")
        # transforming data for curated/consumption layer.
        # preparing data for sales
        sales_dataframe = select_few_columns(df_with_partitioned_columns, sales_dataset_columns)
        output_to_files("data/consumption/sales/", 'parquet', sales_dataframe, "append", "yes")
        logger.info("Sales Output has been written to consumption layer")

        # preparing data for customer data
        # aggregating as well.

        customer_unagg_dataframe = select_few_columns(df_with_partitioned_columns, customer_dataset_columns)
        customer_unagg_dataframe.show()
        agg_customer_df = calculate_running_total(spark, customer_unagg_dataframe)
        output_to_files("data/consumption/customer/", 'parquet', agg_customer_df, "overwrite", "no ")
        logger.info("Aggregated customer Output has been written to consumption layer")
    except Exception as e:
        logger.error("Error in transforming or writing the data ")
        logger.error("Exception: %s" % str(e))



if __name__ == '__main__':
    main()
