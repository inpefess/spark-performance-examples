name := "spark-performance-examples"
version := "1.0"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
