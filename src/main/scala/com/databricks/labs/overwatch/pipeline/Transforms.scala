package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}

trait Transforms extends SparkSessionWrapper {

  import spark.implicits._

  protected val sparkEventsDF: DataFrame = spark.table(s"${Config.databaseName}.spark_events_bronze")
  //    .repartition().cache
  //  sparkEventsDF.count

  // eventlog default path
  // Path == uri:/prefix/<cluster_id>/eventlog/<hostName>.replace("-", "_")/sparkContextId/<eventlog> || <eventlog-yyyy-MM-dd--HH-00.gz>
  def groupFilename(filename: Column): Column = {
    val segmentArray = split(filename, "/")
    val byCluster = array_join(slice(segmentArray, 1, 3), "/").alias("byCluster")
    val byClusterHost = array_join(slice(segmentArray, 1, 5), "/").alias("byDriverHost")
    val bySparkContextID = array_join(slice(segmentArray, 1, 6), "/").alias("bySparkContext")
    struct(byCluster, byClusterHost, bySparkContextID).alias("filnameGroup")
  }

  def SubtractTime(start: Column, end: Column): Column = {
    val runTimeMS = end - start
    val runTimeS = runTimeMS / 1000
    val runTimeM = runTimeS / 60
    val runTimeH = runTimeM / 60
    struct(
      start.alias("startEpochMS"),
      from_unixtime(start / 1000).alias("startTS"),
      end.alias("endEpochMS"),
      from_unixtime(end / 1000).alias("endTS"),
      lit(runTimeMS).alias("runTimeMS"),
      lit(runTimeS).alias("runTimeS"),
      lit(runTimeM).alias("runTimeM"),
      lit(runTimeH).alias("runTimeH")
    ).alias("RunTime")
  }

  // Does not remove null structs
  def removeNullCols(df: DataFrame): (Array[Column], DataFrame) = {
    val cntsDF =  df.summary("count").drop("summary")
    val nonNullCols = cntsDF
      .collect().flatMap(r => r.getValuesMap(cntsDF.columns).filter(_._2 != "0").keys).map(col)
    val complexTypeFields = df.schema.fields
      .filter(f => f.dataType.isInstanceOf[StructType] || f.dataType.isInstanceOf[ArrayType])
      .map(_.name).map(col)
    val cleanDF = df.select(nonNullCols ++ complexTypeFields: _*)
    (nonNullCols ++ complexTypeFields, cleanDF)
  }

  object Executor {

    // TODO -- Can there be multiple instances of ExecutorN in a single spark context?
    def compositeKey: Array[Column] = {
      Array('SparkContextID, col("ExecutorInfo.Host"), 'ExecutorID)
    }

    private val executorAddedDF = sparkEventsDF.filter('Event === "SparkListenerExecutorAdded")
      .select('ExecutorID, 'ExecutorInfo, 'Timestamp.alias("executorAddedTS"), 'filename)

    private val executorRemovedDF = sparkEventsDF.filter('Event === "SparkListenerExecutorRemoved")
      .select('ExecutorID, 'RemovedReason, 'Timestamp.alias("executorRemovedTS"), 'filename)

    val executorDF: DataFrame = executorAddedDF.join(executorRemovedDF, Seq("ExecutorID", "filename"))
      .withColumn("SparkContextID", split('filename, "/")(5))
      .withColumn("filnameGroup", groupFilename('filename))
      .withColumn("TaskRunTime", SubtractTime('executorAddedTS, 'executorRemovedTS))
        .drop("executorAddedTS", "executorRemovedTS")

  }

  object Jobs {

    def compositeKey: Array[Column] = {
      Array('AppID, 'JobID, 'StageID)
    }

    // This is Job level so cannot explode stages yet
    private val jobStartDF = sparkEventsDF.filter('Event.isin("SparkListenerJobStart"))
      .select(
        $"Properties.sparkappid".alias("AppID"), 'JobID,
        'StageIDs, 'SubmissionTime,
        $"Properties.sparksqlexecutionid".alias("JobExecutionID"),
        'Properties.alias("PropertiesAtJobStart"), 'filename.alias("startFilename")
      )
      .withColumn("SparkContextID", split('startFilename, "/")(5))

    private val jobEndDF = sparkEventsDF.filter('Event.isin("SparkListenerJobEnd"))
      .select('JobID, 'JobResult, 'CompletionTime, 'filename.alias("endFilename"))
      .withColumn("SparkContextID", split('endFilename, "/")(5))


    val jobsDF: DataFrame = jobStartDF.join(jobEndDF, Seq("JobId", "SparkContextID"))
      .withColumn("startFilenameGroup", groupFilename('startFilename))
      .withColumn("endFilenameGroup", groupFilename('endFilename))
      .withColumn("JobRunTime", SubtractTime('SubmissionTime, 'CompletionTime))
      .drop("SubmissionTime", "CompletionTime")
  }

}
