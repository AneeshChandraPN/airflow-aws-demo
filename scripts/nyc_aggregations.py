from __future__ import print_function
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("""
        Usage: nyc_aggregations.py <s3_input_path> <s3_output_path> 
        """, file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[1]

    spark = SparkSession\
        .builder\
        .appName("NYC Aggregations")\
        .getOrCreate()

    sc = spark.sparkContext

    df = spark.read.parquet(input_path)
    df.printSchema
    df_out = df.groupBy('pulocationid', 'trip_type', 'payment_type').agg({'fare_amount': 'sum'})

    df_out.write.parquet(output_path)

    spark.stop()

