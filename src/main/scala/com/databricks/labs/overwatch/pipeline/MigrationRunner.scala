package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import com.databricks.labs.overwatch.utils.{BadConfigException, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}

object MigrationRunner extends SparkSessionWrapper{
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    envInit()
    if (args.length == 3) {
      val sourceETLDB = args(0)
      val migrateRootPath = args(1)
      val configPath =  args(2)
      logger.log(Level.INFO,s"Migration Process started from ${sourceETLDB} to ${migrateRootPath} With Config Path set as ${configPath}")
      Migration(sourceETLDB,migrateRootPath,configPath)

    }else if (args.length == 4){
      val sourceETLDB = args(0)
      val migrateRootPath = args(1)
      val configPath =  args(2)
      val tablesToExclude = args(3)
      logger.log(Level.INFO,s"Migration Process started from ${sourceETLDB} to ${migrateRootPath} With Config Path set as ${configPath} " +
        s"and tables excluded from migration are ${tablesToExclude}")
      Migration(sourceETLDB,migrateRootPath,configPath,tablesToExclude)
    }else{
      val errMsg = s"Main class requires 3/4 argument, received ${args.length} arguments. Please review the " +
        s"docs to compose the input arguments appropriately."
      throw new BadConfigException(errMsg)
    }

  }
}
