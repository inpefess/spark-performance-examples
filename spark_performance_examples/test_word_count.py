# coding=utf-8
import os
import shutil
from unittest import TestCase

from pyspark.sql.functions import count, explode, split

from spark_session_getter import get_spark_session


class TestWordCount(TestCase):
    """
        in every test we do absolutely the same:
        1) read all lines of a simple text file into RDD/DataFrame
        2) split every line into word list (by space character)
        3) find counts of occurrences of every word in the text
        4) save resulting word counts to a new text file
    """

    output_path = os.environ["DATA_DIR"] + "/dump"
    input_path = os.environ["DATA_DIR"] + "/text.txt"

    def setUp(self):
        # every test will create this directory so we drop it beforehand
        shutil.rmtree(path=self.output_path, ignore_errors=True)
        # we have a separate SparkSession for every test
        self.spark = get_spark_session()

    def tearDown(self):
        # we have a separate SparkSession for every test
        self.spark.stop()

    def test_rdd_group_by_key_wordcount(self):
        # using groupByKey is usually considered the worst style
        self.spark.sparkContext \
            .textFile(self.input_path) \
            .flatMap(lambda row: row.split(' ')) \
            .map(lambda word: (word, 1)) \
            .groupByKey() \
            .map(lambda word_ones: (word_ones[0], sum(word_ones[1]))) \
            .saveAsTextFile(self.output_path)

    def test_reduce_by_key(self):
        # this is nearly the same as the previous test but is using reduceByKey
        # which is much better
        self.spark.sparkContext \
            .textFile(self.input_path) \
            .flatMap(lambda row: row.split(' ')) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda x, y: x + y) \
            .saveAsTextFile(self.output_path)

    def test_data_frame_wordcount(self):
        # and the best decision for PySpark is using DataFrames over RDDs
        self.spark.read \
            .text(self.input_path) \
            .select(explode(split("value", ' '))) \
            .groupBy("col") \
            .agg(count("col")) \
            .write.csv(self.output_path)
