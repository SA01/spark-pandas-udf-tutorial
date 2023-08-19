from pyspark import SparkConf
from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    conf = (
        SparkConf().set("spark.driver.memory", "8g")
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
        .set("spark.sql.execution.arrow.maxRecordsPerBatch", "2")
    )

    spark_session = SparkSession\
        .builder\
        .master("local[1]")\
        .config(conf=conf)\
        .appName(app_name) \
        .getOrCreate()

    spark_session.sparkContext.setCheckpointDir("checkpoint")

    return spark_session
