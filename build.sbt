name := "overwatch"

organization := "com.databricks.labs"

version := "0.7.2.0"

scalaVersion := "2.12.12"
scalacOptions ++= Seq("-Xmax-classfile-name", "78")

Test / fork  := true
Test / envVars := Map("OVERWATCH_ENV" -> " ","OVERWATCH_TOKEN" -> " ","OVERWATCH" -> " ")

val sparkVersion = "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
libraryDependencies += "com.databricks" % "dbutils-api_2.12" % "0.0.5" % Provided
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.595" % Provided
libraryDependencies += "io.delta" % "delta-core_2.12" % "1.0.0" % Provided
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"

//libraryDependencies += "org.apache.hive" % "hive-metastore" % "2.3.9"

libraryDependencies += "com.microsoft.azure" %% "azure-eventhubs-spark" % "2.3.21" % Provided
libraryDependencies += "com.microsoft.azure" % "msal4j" % "1.10.1" % Provided exclude("com.fasterxml.jackson.core", "jackson-databind")
libraryDependencies += "com.databricks.labs" %% "dataframe-rules-engine" % "0.2.0"

libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % Test
libraryDependencies += "org.mockito" % "mockito-core" % "3.5.15" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

Compile / run := Defaults.runTask(Compile / fullClasspath ,Compile/ run / mainClass, Compile / run / runner).evaluated
Compile / runMain  := Defaults.runMainTask(Compile / fullClasspath,Compile / run /  runner ).evaluated

// groupId, SCM, license information
homepage := Some(url("https://github.com/databrickslabs/overwatch"))
scmInfo := Some(ScmInfo(url("https://github.com/databrickslabs/overwatch"), "git@github.com:databrickslabs/overwatch.git"))
developers := List(Developer("geeksheikh", "Daniel Tomes", "daniel@databricks.com", url("https://github.com/GeekSheikh")))
licenses += ("Databricks", url("https://github.com/databrickslabs/overwatch/blob/develop/LICENSE"))
publishMavenStyle := true
Test / parallelExecution := false //TO avoid object collision happening in PipelineFunctionsTest

publishTo := Some(
  if (version.value.endsWith("SNAPSHOT"))
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
 assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)