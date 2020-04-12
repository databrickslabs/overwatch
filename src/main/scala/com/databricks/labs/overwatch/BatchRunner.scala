package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Pipeline, Initializer}
import com.databricks.labs.overwatch.utils.Global._
import org.apache.log4j.{Level, Logger}

object BatchRunner {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val params: OverwatchParams = if (args.length != 0) {
      Initializer.validateBatchParams(args)
    } else { OverwatchParams(None, None) }

    val (workspace, database, pipeline) = Initializer(params)

    pipeline

    val initializer = Initializer(params)
//    val appender = Appender(workspace, database)
//    appender.appendJobs("holder")
//    workspace.getEventsByCluster("0321-201717-chows241").show()
    workspace.getClustersDF.show()
//    workspace.getJobsDF.printSchema()

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
