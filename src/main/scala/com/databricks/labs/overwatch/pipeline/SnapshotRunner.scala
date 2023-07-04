package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.overwatch.utils.{BadConfigException, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}


object SnapshotRunner extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    envInit()
    if (args.length == 3) { //Snapshot with default pipeline, tablesToExclude and  cloneLevel
      val sourceETLDB = args(0)
      val targetPrefix = args(1)
      val snapshotType = args(2)
      logger.log(Level.INFO, s"${snapshotType} Snapshot Process for Bronze, Silver and Gold started from ${sourceETLDB} to ${targetPrefix} ")
      Snapshot(sourceETLDB, targetPrefix, snapshotType)
    } else if (args.length == 4) {
      val sourceETLDB = args(0)
      val targetPrefix = args(1)
      val snapshotType = args(2)
      val pipeline = args(3)
      logger.log(Level.INFO, s"${snapshotType} Snapshot Process for ${pipeline} started from ${sourceETLDB} to ${targetPrefix} ")
      Snapshot(sourceETLDB, targetPrefix, snapshotType, pipeline)
    } else if (args.length == 5) {
      val sourceETLDB = args(0)
      val targetPrefix = args(1)
      val snapshotType = args(2)
      val pipeline = args(3)
      val tablesToExclude = args(4)
      logger.log(Level.INFO, s"${snapshotType} Snapshot Process for ${pipeline} started from ${sourceETLDB} to ${targetPrefix} with table excluded ${tablesToExclude}")
      Snapshot(sourceETLDB, targetPrefix, snapshotType, pipeline, tablesToExclude)
    } else if (args.length == 6) {
      val sourceETLDB = args(0)
      val targetPrefix = args(1)
      val snapshotType = args(2)
      val pipeline = args(3)
      val tablesToExclude = args(4)
      val cloneLevel = args(5)
      logger.log(Level.INFO, s"${snapshotType} Snapshot Process for ${pipeline} started with cloing level ${cloneLevel} from ${sourceETLDB} to ${targetPrefix} with table excluded ${tablesToExclude}")
      Snapshot(sourceETLDB, targetPrefix, snapshotType, pipeline, tablesToExclude, cloneLevel)
    } else {
      val errMsg = s"Main class requires 3/4/5/6 argument, received ${args.length} arguments. Please review the " +
        s"docs to compose the input arguments appropriately."
      throw new BadConfigException(errMsg)
    }
  }
}
