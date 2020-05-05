package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, Initializer, Pipeline}
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BatchRunner extends SparkSessionWrapper{

  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

//    spark.sql("drop database if exists overwatch cascade")
    import spark.implicits._

    val (workspace, database) = if (args.length != 0) {
      Initializer(args)
    } else {
      Initializer(Array())
    }

    //    pipeline.buildBronze()
//    Bronze(workspace, database).run()

    val w = Window.partitionBy('moduleID).orderBy('untilTS)
    val untilTSByModule = spark.table("overwatch.pipeline_report")
      .withColumn("rnk", rank().over(w))
      .withColumn("rn", row_number().over(w))
      .filter('rnk === 1 && 'rn === 1)
      .select('moduleID, 'untilTS)
      .rdd.map(r => (r.getInt(0), r.getLong(1)))
      .collectAsMap()

    println(untilTSByModule(1002))


    // Test
    // Post with paginate
//    workspace.getEventsByCluster("0318-151752-abed99").show()

    // Post Without Paginate
    //    workspace.getJobsDF.show()

    // Get with query No paginate
    //    workspace.getDBFSPaths("/Users").show()

    // Get with Query with paginate
    //    ApiCall("jobs/runs/list", Some(Map(
    //      "job_id" -> 20346,
    //      "limit" -> 2
    //    ))).executeGet().asDF.show()

    // Create target database if not exists
    def initializeTargets = ???

    // Append all batch data to target tables
    def updateTargets = ???

    def updateReportingTables = ???

    // pull in the data from the stream job and merge into master insights
    def mergeStreamingAndBatchInsights = ???

    // Data queried often, as identified by stream runner, profile it and look for opportunities
    def profileFrequentData = ???

    def deliverKeyInsights = ???

    //    tempTester(params)

  }
}
