name := "overwatch"

organization := "com.databricks.labs"

version := "0.3_RC1"

scalaVersion := "2.12.10"
scalacOptions ++= Seq("-Xmax-classfile-name", "78")

val sparkVersion = "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
libraryDependencies += "com.databricks" %% "dbutils-api" % "0.0.5" % Provided
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.595" % Provided
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"

libraryDependencies += "com.microsoft.azure" %% "azure-eventhubs-spark" % "2.3.18" % Provided

//libraryDependencies += "io.delta" %% "delta-core" % "0.6.1"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % Test
// https://mvnrepository.com/artifact/org.mockito/mockito-core
libraryDependencies += "org.mockito" % "mockito-core" % "3.5.15" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test
// https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.0.0_1.0.0" % Test

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated


// enforce execution of tests during packaging - uncomment next line when we fix dependencies
// Keys.`package` := (Compile / Keys.`package` dependsOn Test / test).value

//coverageEnabled := true
//coverageMinimum := 80
//coverageFailOnMinimum := true

// exclude scala-library dependency
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
