# Spark Performance Examples
I tried to explore some Spark performance tuning on a classic example - counting words in a large text.
For the source of a underlying corpus I have chosen reviews from [YELP dataset](https://www.yelp.com/dataset_challenge).

## What I have already tried
* RDD.groupByKey in Python and Scala
* RDD.reduceByKey in Python and Scala
* DataFrame and SparkSQL functions in Python and Scala

## What I want to do next
Python UDFs vs Java UDFs in PySpark.

In Spark 2.1 there is an option to [register Java UDFs](https://issues.apache.org/jira/browse/SPARK-11775)
and it is known that using a Python UDF with DataFrames leads to the same shortcomings as when using RDDs in PySpark.

## How to use this code
1. get the YELP dataset [here](https://www.yelp.com/dataset_challenge/dataset)
1. put it into a `$DATA_DIR` and define this environment variable
1. run `prepare_text.py` script (and yes, I use Python 3 for this project)
1. run `py.test --duration=3` to see PySpark timings
1. run `sbt test` to see Scala timings

You can also use Idea/PyCharm or your favourite IDE/text editor for running these UnitTests.

You need some Spark running (local is enough - all code runs for less that 15 minutes).
