
name := "overwatch"

organization := "com.databricks.labs"

version := "0.4.2"

scalaVersion := "2.12.12"
scalacOptions ++= Seq("-Xmax-classfile-name", "78")

val sparkVersion = "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
libraryDependencies += "com.databricks" %% "dbutils-api" % "0.0.5" % Provided
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.595" % Provided
libraryDependencies += "io.delta" % "delta-core_2.12" % "0.8.0" % Provided
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"

libraryDependencies += "com.microsoft.azure" %% "azure-eventhubs-spark" % "2.3.18" % Provided
libraryDependencies += "com.databricks.labs" %% "dataframe-rules-engine" % "0.1.2" % Provided

libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % Test
libraryDependencies += "org.mockito" % "mockito-core" % "3.5.15" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated

// groupId, SCM, license information
homepage := Some(url("https://github.com/databrickslabs/overwatch"))
scmInfo := Some(ScmInfo(url("https://github.com/databrickslabs/overwatch"), "git@github.com:databrickslabs/overwatch.git"))
developers := List(Developer("geeksheikh", "Daniel Tomes", "daniel@databricks.com", url("https://github.com/GeekSheikh")))
licenses += ("Databricks", url("https://github.com/databrickslabs/overwatch/blob/develop/LICENSE"))
publishMavenStyle := true

publishTo := Some(
  if (version.value.endsWith("SNAPSHOT"))
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

// enforce execution of tests during packaging - uncomment next line when we fix dependencies
// Keys.`package` := (Compile / Keys.`package` dependsOn Test / test).value

//coverageEnabled := true
//coverageMinimum := 80
//coverageFailOnMinimum := true

// exclude scala-library dependency
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
