package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Pipeline, Initializer}
import org.apache.log4j.{Level, Logger}

object BatchRunner {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val (workspace, database, pipeline) = if (args.length != 0) {
      Initializer(args)
    } else { Initializer(Array()) }

//    pipeline.buildBronze()

//    Play
    workspace.getJobsDF.show()
//    workspace.getJobsDF.show()

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
