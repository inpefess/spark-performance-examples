# coding=utf-8
import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType


def get_spark_session() -> SparkSession:
    """
    a functions that returns pre-configured SparkSession
    :return: new pre-configured SparkSession
    """
    # this configuration could be changed for a cluster
    spark_conf = SparkConf() \
        .setMaster("local[*]") \
        .set("spark.sql.shuffle.partitions", "12") \
        .set(
        "spark.driver.extraClassPath",
        os.path.dirname(os.path.abspath(__file__)) +
        "/../target/scala-2.11/*"
    )
    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()
    # here we register too UDFs that do absolutely the same
    # but are implemented in different languages
    spark.udf.register(
        "pythonSplit", lambda s: s.split(' '), ArrayType(StringType())
    )
    spark.udf.sqlContext.registerJavaFunction(
        "scalaSplit", "ScalaSplit", ArrayType(StringType())
    )
    return spark
