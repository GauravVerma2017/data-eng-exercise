import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, coalesce
def read_input_source_1(run_date):
    return spark.read.json("/Users/gverma/PycharmProjects/data-eng-exercise/input_source_1/data_{run_date}.json".format(run_date=run_date))

def read_input_source_2(run_date):
    return spark.read.option("header", True).csv("/Users/gverma/PycharmProjects/data-eng-exercise/input_source_2/engagement_{run_date}.csv".format(run_date=run_date))

def merge_data(source_1_DF, source_2_DF, run_date):
    return source_1_DF.join(source_2_DF, "post_id").\
        withColumn("date", lit(run_date)).\
        select(col("date"),
               col("post_id"),
               coalesce(col("shares.count"), lit(0)).alias("shares").cast("long"),
               coalesce(col("comments"), lit(0)).cast("long").alias("comments"),
               coalesce(col("likes"), lit(0)).cast("long").alias("likes")
               )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True, help="pass the --run_date yyyymmdd")
    args = vars(parser.parse_args())

    spark = SparkSession.builder.appName("file_merger").master("local").getOrCreate()

    source_1_DF = read_input_source_1(args["run_date"])
    source_2_DF = read_input_source_2(args["run_date"])
    source_1_DF.printSchema()
    source_2_DF.printSchema()
    output_DF = merge_data(source_1_DF, source_2_DF, args["run_date"])
    output_DF.write.mode("append").csv("/Users/gverma/PycharmProjects/data-eng-exercise/output/{run_date}".format(run_date=args["run_date"]))