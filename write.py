import sys

def output_to_files(file_name, target_format, df, mode_of_writing, partitionRequired):
    try:
       if (partitionRequired == "yes"):
         df.write.partitionBy('OrderYear', 'OrderMonth', 'OrderDay'). \
            mode(mode_of_writing). \
            format(target_format). \
            save(file_name)
       else:
        df.write. \
            mode(mode_of_writing). \
            format(target_format). \
            save(file_name)

    except Exception as e:
        print("Error in writing data ")
        print("Exception: %s" % str(e))
        sys.exit(1)
