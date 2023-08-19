from pyspark.sql import SparkSession
from pyspark.sql.pandas.functions import pandas_udf

from common import create_spark_session
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pandas as pd
import random
import time


@pandas_udf(T.DoubleType())
def km_to_miles(km_data: pd.Series) -> pd.Series:
    conversation_rate_multiplier = 0.62137119
    return km_data * conversation_rate_multiplier


@F.udf(returnType=T.FloatType())
def km_to_miles_udf(km_data_item: int) -> float:
    conversation_rate_multiplier = 0.62137119
    return km_data_item * conversation_rate_multiplier


def data_generator(spark: SparkSession, num_items: int, max_number: int):
    data = [[random.Random().randint(a=0, b=max_number)] for _ in range(1, num_items)]
    data_df = spark.createDataFrame(data=data, schema=["data"])

    return data_df


def simple_test():
    spark = create_spark_session("simple panda udf")
    test_data = data_generator(spark=spark, num_items=10, max_number=100)
    test_data.show(truncate=False)

    result_data = test_data.withColumn("in_km", km_to_miles(F.col("data")))
    result_data.show(truncate=False)


def performance_test(run_pandas_udf: bool):
    spark = create_spark_session("simple panda udf")
    # test_data = data_generator(spark=spark, num_items=5000000, max_number=100).repartition(128).cache()
    # test_data.write.parquet("data/perf_test_data/test_data_5000000")

    test_data_path = "data/perf_test_data/test_data_5000000/*.parquet"
    num_tries = 10
    print("Starting test")

    if run_pandas_udf:
        start_time = time.time()
        for i in range(1, num_tries + 1):
            run_start = time.time()
            print(f"Running test {i}")
            test_data = spark.read.parquet(test_data_path)
            result_data = test_data.withColumn("in_km", km_to_miles(F.column("data")))
            result_data.write.parquet(f"output/perf_test_output/pandas_udf_run_{i}")
            print(f"Run {i} took {time.time() - run_start} seconds")

        res_1_time = time.time() - start_time
        print(f"Pandas UDF took {res_1_time} seconds")

    else:
        start_time = time.time()
        for i in range(1, num_tries + 1):
            run_start = time.time()
            print(f"Running test {i}")
            test_data = spark.read.parquet(test_data_path)
            result_data = test_data.withColumn("in_km", km_to_miles_udf(F.column("data")))
            result_data.write.parquet(f"output/perf_test_output/python_udf_run_{i}")
            print(f"Run {i} took {time.time() - run_start} seconds")

        res_2_time = time.time() - start_time
        print(f"Python UDF took {res_2_time} seconds")


if __name__ == '__main__':
    simple_test()
    # performance_test(True)
    # performance_test(False)
