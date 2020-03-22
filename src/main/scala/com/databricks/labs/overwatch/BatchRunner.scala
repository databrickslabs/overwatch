package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.Initializer
import com.databricks.labs.overwatch.utils.GlobalStructures._
import org.apache.log4j.{Level, Logger}

object BatchRunner {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def tempTester(params: OverwatchParams): Unit = {

    val workspace = Workspace(params)
    val jobsDF = workspace.getJobs
    jobsDF.show()
  }

  def main(args: Array[String]): Unit = {

    val params: OverwatchParams = if (args.length != 0) {
      Initializer.validateBatchParams(args)
    } else { OverwatchParams(None, None) }

    // Create target database if not exists
    def initializeTargets = ???

    // Append all batch data to target tables
    def updateTargets = ???

    def updateReportingTables = ???

    // pull in the data from the stream job and merge into master insights
    def mergeStreamingAndBatchInsights = ???

    def deliverKeyInsights = ???

    tempTester(params)

  }
}
