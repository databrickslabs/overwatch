name := "Overwatch"

organization := "com.databricks.labs"

version := "0.2"

scalaVersion := "2.11.12"
scalacOptions ++= Seq("-Xmax-classfile-name", "78")

//unmanagedBase := new java.io.File("c:\\dev\\software\\anaconda\\envs\\ml37\\lib\\site-packages\\pyspark\\jars")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.5"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.6"
libraryDependencies += "com.databricks" % "dbutils-api_2.11" % "0.0.4"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.4.2"
libraryDependencies += "com.microsoft.azure" %% "azure-eventhubs-spark" % "2.3.7"
//libraryDependencies += "io.delta" %% "delta-core" % "0.6.1"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test"

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { f =>
    f.data.getName.contains("spark-core") ||
      f.data.getName.contains("spark-sql") ||
      f.data.getName.contains("spark-hive") ||
      f.data.getName.contains("azure-eventhubs-spark") ||
      f.data.getName.contains("delta-core") ||
//      f.data.getName.contains("com.databricks.backend") ||
      f.data.getName.contains("com.databricks.dbutils-api_2.11")
  }
}