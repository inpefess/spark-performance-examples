import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, count, explode, split}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.FunSuite

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

class WordCountTest extends FunSuite {
  private val inputDir = System.getenv("DATA_DIR") + "/text.txt"
  private val outputDir = System.getenv("DATA_DIR") + "/dump"

  // this is a so called 'fixture' - a functional way to do setUp with side-effects for UnitTests
  private def spark = {
    // for the purpose of these tests one doesn't want to see verbose logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val prop = new Properties
    prop.load(new FileInputStream("src/spark.properties"))
    val sparkConf = new SparkConf().setAll(prop.asScala)
    val spark = SparkSession.builder.config(conf = sparkConf).getOrCreate()
    spark
  }

  /*
    in every test we do absolutely the same:
    1) read all lines of a simple text file into RDD/DataFrame
    2) split every line into word list (by space character)
    3) find counts of occurrences of every word in the text
    4) save resulting word counts to a new text file
  */

  test("test RDD groupByKey word count") {
    // using groupByKey is usually considered the worst style
    spark.sparkContext
      .textFile(inputDir)
      .flatMap(_.split(' '))
      .map((_, 1))
      .groupByKey()
      .map(word_ones => (word_ones._1, word_ones._2.sum))
      .saveAsTextFile(outputDir)
  }

  test("test RDD reduceByKey word count") {
    // this is nearly the same as the previous test but is using reduceByKey which is much better
    spark.sparkContext
      .textFile(inputDir)
      .flatMap(_.split(' '))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(outputDir)
  }

  test("test DataFrame word count") {
    // in Scala this code using DataFrame should be nearly the same as the previous one
    spark.read
      .text(inputDir)
      .select(explode(split(col("value"), " ")))
      .groupBy("col")
      .agg(count("col"))
      .write.mode(SaveMode.Overwrite)
      .csv(outputDir)
  }
}
