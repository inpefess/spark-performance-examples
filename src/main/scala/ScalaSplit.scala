import org.apache.spark.sql.api.java.UDF1

/*
  This is an extremely simple UDF that work similarly to org.apache.spark.sql.functions.split
 */
class ScalaSplit extends UDF1[String, Array[String]] {
  @throws[Exception]
  def call(str: String): Array[String] = str.split(" ")
}
