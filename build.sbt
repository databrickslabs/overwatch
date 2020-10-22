name := "Overwatch"

organization := "com.databricks.labs"

version := "0.2"

scalaVersion := "2.11.12"
scalacOptions ++= Seq("-Xmax-classfile-name", "78")

// default value
val jarsPathManual = ""
// SATEESH DBR DEP PATH
// val jarsPathManual = "/usr/local/anaconda3/envs/dbconnectdbr66/lib/python3.7/site-packages/pyspark/jars"
// TOMES DBR DEP PATH
// val jarsPathManual = "c:\\dev\\software\\anaconda\\envs\\ml37\\lib\\site-packages\\pyspark\\jars"

// TODO -- alter code that requires this for build and get back to core spark, not dbr for build
unmanagedBase := {
  import java.nio.file.{Files, Paths}
  import scala.sys.process._

  val jarsPathEnv = System.getenv("DBCONNECT_JARS")
  if (jarsPathEnv != null && Files.isDirectory(Paths.get(jarsPathEnv))) {
    // println("We have path from the environment variable! " + jarsPathEnv)
    new java.io.File(jarsPathEnv)
  } else {
    val paramPathEnv = System.getProperty("DbConnectJars")
    if (paramPathEnv != null && Files.isDirectory(Paths.get(paramPathEnv))) {
      // println("We have path from the system parameter! " + paramPathEnv)
      new java.io.File(paramPathEnv)
    } else {
      val dbConenctPath: String = try {
        Seq("databricks-connect", "get-jar-dir").!!.trim
      } catch {
        case e: Exception =>
          // println(s"Exception running databricks-connect: ${e.getMessage}")
          ""
      }
      if (!dbConenctPath.isEmpty && Files.isDirectory(Paths.get(dbConenctPath))) {
        // println("We have path from the databricks-connect! " + dbConenctPath)
        new java.io.File(dbConenctPath)
      } else if (Files.isDirectory(Paths.get(jarsPathManual))) {
        // println("We have path from the manual path! " + jarsPathManual)
        new java.io.File(jarsPathManual)
      } else {
        throw new RuntimeException("Can't find DB jars required for build! Set DBCONNECT_JARS environment variable, set -DDbConnectJars=path in .sbtopts, activate conda environment, or set the 'jarsPathManual' variable")
      }
    }
  }
}

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.5"

libraryDependencies += "com.databricks" % "dbutils-api_2.11" % "0.0.4"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.4.2"
libraryDependencies += "com.microsoft.azure" %% "azure-eventhubs-spark" % "2.3.7"
//libraryDependencies += "io.delta" %% "delta-core" % "0.6.1"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % Test
// https://mvnrepository.com/artifact/org.mockito/mockito-core
libraryDependencies += "org.mockito" % "mockito-core" % "3.5.15" % Test
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.2.2" % Test


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
