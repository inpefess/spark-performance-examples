# Spark Performance Examples
I tried to explore some Spark performance tuning on a classic example - counting words in a large text.
For the source of an underlying corpus I have chosen reviews from [YELP dataset](https://www.yelp.com/dataset_challenge).

## What I have already tried
* RDD.groupByKey in Python and Scala
* RDD.reduceByKey in Python and Scala
* DataFrame with built-in SparkSQL functions in Python and Scala
* DataFrame with Python and Java UDFs in PySpark 

## How to use this code
1. get the YELP dataset [here](https://www.yelp.com/dataset_challenge/dataset)
1. extract the archive into a `$DATA_DIR` folder and define respective environment variable
1. run `sbt package` to create a JAR file with Scala UDF
1. run `pyspark_performance_examples/prepare_text.py` script (and yes, I use Python 3 for this project)
1. run `py.test --duration=5` in `pyspark_performance_examples` directory to see PySpark timings
1. run `sbt test` to see Scala timings

You can also use Idea/PyCharm or your favourite IDE/text editor for running these UnitTests.

You need some Spark running (local is enough - all tests run for less than 10 minutes on 4 cores and 8 GB RAM).
