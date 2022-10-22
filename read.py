import sys


def read_files(spark,data_dir,file_name,source_format):
    try:
       df = spark.read.format(source_format).options(header=True).load(f'{data_dir}/{file_name}')
    except Exception as e:
        print("Error in reading the input files")
        print("Exception: %s" % str(e))
        sys.exit(1)
    return df