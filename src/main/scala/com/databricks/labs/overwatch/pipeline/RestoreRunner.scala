package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.overwatch.utils.{BadConfigException, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}


object RestoreRunner extends SparkSessionWrapper{
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    envInit()
    if (args.length == 2){
      val sourcePrefix = args(0)
      val targetPrefix = args(1)

      logger.log(Level.INFO,s"Restoration process started from ${sourcePrefix} to ${targetPrefix}")
      Restore(sourcePrefix,targetPrefix)
    }else{
      val errMsg = s"Main class requires 2 argument, received ${args.length} arguments. Please review the " +
        s"docs to compose the input arguments appropriately."
      throw new BadConfigException(errMsg)
    }
  }
}
