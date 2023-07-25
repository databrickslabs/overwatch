package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.overwatch.utils.{BadConfigException, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}


object SnapshotRunner extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    envInit()
    if (args.length >= 3 && args.length <= 6) {
      val sourceETLDB = args(0)
      val targetPrefix = args(1)
      val snapshotType = args(2)
      val pipeline = args.lift(3).getOrElse("Bronze,Silver,Gold")
      val tablesToExclude = args.lift(4).getOrElse("")
      val cloneLevel  = args.lift(4).getOrElse("DEEP")
      logger.log(Level.INFO, s"${snapshotType} Snapshot Process for ${pipeline} started with cloing level ${cloneLevel} from ${sourceETLDB} to ${targetPrefix} with table excluded ${tablesToExclude}")
      Snapshot(sourceETLDB, targetPrefix, snapshotType, pipeline, tablesToExclude, cloneLevel)
    } else {
      val errMsg = s"Main class requires 3/4/5/6 argument, received ${args.length} arguments. Please review the " +
        s"docs to compose the input arguments appropriately."
      throw new BadConfigException(errMsg)
    }
  }
}
