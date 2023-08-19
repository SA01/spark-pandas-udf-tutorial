from typing import Iterator, Tuple

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.pandas.functions import pandas_udf
import pandas as pd

from common import create_spark_session


@pandas_udf(returnType=T.IntegerType())
def trip_duration(start_time: pd.Series, end_time: pd.Series) -> pd.Series:
    print(start_time)
    print(end_time)
    result = (end_time - start_time).dt.seconds
    print(result)
    print("!=====================================!")
    return result


@pandas_udf(returnType=T.IntegerType())
def trip_duration_alt(start_and_end_times: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    for start_times, end_times in start_and_end_times:
        print(start_times)
        print(end_times)
        print("!=====================================!")
        yield (end_times - start_times).dt.seconds


def calculate_trip_duration(spark: SparkSession):
    trips_data = spark.read.parquet("data/ny_taxi_trips/*.parquet").limit(20).repartition(5)

    result_data = trips_data.withColumn(
        "duration", trip_duration_alt(F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime"))
    )

    result_data.show(truncate=False)


@pandas_udf(T.DoubleType())
def average_udf(values: pd.Series) -> float:
    return values.mean()


def calculate_avg_trip_duration(spark: SparkSession):
    trips_data = spark.read.parquet("data/ny_taxi_trips/*.parquet").limit(20).repartition(5)

    result_data = trips_data.withColumn(
        "duration", trip_duration_alt(F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime"))
    ).groupby("VendorID").agg(average_udf(F.col("duration")).alias("avg_duration"))

    result_data.show(truncate=False)


@pandas_udf(returnType=T.FloatType())
def tip_percent_of_fare(fares_breakdown: pd.DataFrame) -> pd.Series:
    print(fares_breakdown)
    result = (fares_breakdown['tip_amount']/fares_breakdown['total_amount'] * 100)
    print("!============================================!")
    return result


def calculate_tip_percent(spark: SparkSession):
    trips_data = spark.read.parquet("data/ny_taxi_trips/*.parquet").limit(20).repartition(5)

    result_data = (
        trips_data
        .withColumn('fares', F.struct('total_amount', 'tip_amount'))
        .withColumn("tip_percent", tip_percent_of_fare(F.col("fares")))
    )
    result_data.show(truncate=False)


@pandas_udf(returnType=T.TimestampType())
def add_5_min(time_iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    num_series = 0
    print("!--------------------------------------!")
    for item in time_iterator:
        num_series += 1
        print(num_series)
        print(item)
        yield item + pd.Timedelta(minutes=5)


def add_5_min_test(spark: SparkSession):
    trips_data = spark.read.parquet("data/ny_taxi_trips/*.parquet").limit(20).repartition(5)

    result_data = (
        trips_data
        .withColumn("five_min_after_start_time", add_5_min(F.col("tpep_pickup_datetime")))
    )
    result_data.show(truncate=False)


if __name__ == '__main__':
    spark = create_spark_session("Pandas UDF tutorials")
    # calculate_trip_duration(spark=spark)
    # calculate_tip_percent(spark=spark)
    # add_5_min_test(spark=spark)
    calculate_avg_trip_duration(spark=spark)

