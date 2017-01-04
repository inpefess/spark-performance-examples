# coding=utf-8
import os

from spark_session_getter import get_spark_session

# our text corpus is generated from YELP dataset reviews
# here we just read a JSON file and extract only 'text' field from it
# then we save everything to a text file - one line per review
spark = get_spark_session()
corpus = spark.read \
    .json(os.environ["DATA_DIR"] + "/yelp_academic_dataset_review.json") \
    .select("text") \
    .coalesce(1).write \
    .text(os.environ["DATA_DIR"] + "/text.txt")
spark.stop()
