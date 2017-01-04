# coding=utf-8
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """
    a functions that returns pre-configured SparkSession
    :return: new pre-configured SparkSession
    """
    # this configuration could be changed for a cluster
    spark_conf = SparkConf() \
        .setMaster("local[*]") \
        .set("spark.sql.shuffle.partitions", "12")
    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()
    return spark
