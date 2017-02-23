import org.apache.spark.sql.api.java.UDF1

/**
  * This is an extremely simple UDF that work similarly to org.apache.spark.sql.functions.split
  */
class ScalaSplit extends UDF1[String, Array[String]] {
  /**
    * a wrapper of a built-in split method
    * @param str a string to split by a whitespace symbol
    * @throws it just must throw some exception to be register in Spark
    * @return an array of strings
    */
  @throws[Exception]
  def call(str: String): Array[String] = str.split(" ")
}
