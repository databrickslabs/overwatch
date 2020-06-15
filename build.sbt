name := "Overwatch"

organization := "com.databricks.labs"

version := "0.1_nubank"

scalaVersion := "2.11.12"
scalacOptions ++= Seq("-Xmax-classfile-name", "78")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.5"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.6"
libraryDependencies += "com.databricks" % "dbutils-api_2.11" % "0.0.4"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.4.2"

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { f =>
    f.data.getName.contains("spark-core") ||
      f.data.getName.contains("spark-sql") ||
      f.data.getName.contains("spark-hive") ||
//      f.data.getName.contains("com.databricks.backend") ||
      f.data.getName.contains("com.databricks.dbutils-api_2.11")
  }
}