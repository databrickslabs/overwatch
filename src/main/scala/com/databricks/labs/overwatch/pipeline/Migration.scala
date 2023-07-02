package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.MultiWorkspaceDeployment
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import com.databricks.labs.overwatch.utils.Helpers.removeTrailingSlashes
import com.databricks.labs.overwatch.MultiWorkspaceDeployment._
import org.apache.spark.sql.functions._

/**
 * Class for Migrating the ETL Database to Source to Target and update the config as per the target databases(both consumer and Etl Database)
 * @param _sourceETLDB        ETL Database for Souce from where Migration need to be done.
 * @param _targetPrefix       Target Path for where Migration would be done
 * @param _configPath         path for Configuration file for which migration need to be performed.
 */

class Migration(_sourceETLDB: String, _targetPrefix: String, _configPath: String) {

  private val target_storage_prefix = removeTrailingSlashes(_targetPrefix)
  private val sourceETLDB = _sourceETLDB
  private val configPath = _configPath

  private[overwatch] def updateConfig(): Unit = {
    try {
      if (configPath.toLowerCase().endsWith(".csv")) { // CSV file
        println(s"Config source: csv path ${configPath}")
        if (!Helpers.pathExists(configPath)) {
          throw new BadConfigException("Unable to find config file in the given location:" + configPath)
        }
        val tempConfigPath = (configPath.split("/").dropRight(1) :+ "tempConfig.csv").mkString("/")
        val df = spark.read.format("csv")
          .option("header", "true")
          .option("ignoreLeadingWhiteSpace", true)
          .option("ignoreTrailingWhiteSpace", true)
          .load(configPath)
          .withColumn("storage_prefix", when(col("etl_database_name") === lit(sourceETLDB), lit(target_storage_prefix)).otherwise(col("storage_prefix")))
          .coalesce(1)
          try{
            df.write
              .format("csv")
              .option("header", "true")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .save(tempConfigPath)
          }catch {
            case e: Exception =>
              println(s"Exception while writing to tempConfigPath,Please ensure we have proper write access to ${tempConfigPath}")
              throw e
          }
        val filePath = dbutils.fs.ls(tempConfigPath).last.path
        dbutils.fs.cp(filePath, configPath, true)
        dbutils.fs.rm(tempConfigPath, true)
      } else if (configPath.contains("/")) { // delta path
        println(s"Config source: delta path ${configPath}")
        if (!Helpers.pathExists(configPath)) {
          throw new BadConfigException("Unable to find config file in the given location:" + configPath)
        }
        val configUpdateStatement =
          s"""
      update delta.`$configPath`
      set
        storage_prefix = '$target_storage_prefix'
      Where etl_database_name = '$sourceETLDB'
      """
        spark.sql(configUpdateStatement)
      }else{ // delta table
        println(s"Config source: delta table ${configPath}")
        if (!spark.catalog.tableExists(configPath)) {
          throw new BadConfigException("Unable to find Delta table" + configPath)
        }
        val configUpdateStatement =
          s"""
      update `$configPath`
      set
        storage_prefix = '$target_storage_prefix'
      Where etl_database_name = '$sourceETLDB'
      """
        spark.sql(configUpdateStatement)
      }
    }catch {
      case e: Exception =>
        throw new BadConfigException("Exception while reading config , please provide config csv path/config delta path/config delta table")
    }
  }

}

object Migration extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Create a backup of the Overwatch datasets
   *
   * @param sourceETLDB         Source Database name or ETlDataPathPrefix name.
   * @param migrateRootPath     Target path to where migration need to be performed.
   * @param configPath          Configuration Path where the config file for the source is present. Path can be CSV file or delta path or delta table.
   * @param tablesToExclude     Array of table names to exclude from the snapshot
   *                            this is the table name only - without the database prefix. By Default it is empty.
   * @return
   */

  def apply(
             sourceETLDB: String,
             migrateRootPath:String,
             configPath: String,
             tablesToExclude :String = " "
           ): Unit = {

    val cloneLevel = "Deep"

    val configDF = new MultiWorkspaceDeployment().generateBaseConfig(configPath).filter(col("etl_database_name") === sourceETLDB)
    if (configDF.select("storage_prefix").distinct().count() != 1){
      throw new BadConfigException("Migration is not possible where multiple different storage_prefix present for etl_database_name")
    }
    val pipeline = "Bronze,Silver,Gold"
    val etlDatabase = sourceETLDB
    val consumerDatabase = configDF.select("consumer_database_name").distinct().collect()(0)(0)


    // Step 01 - Start Migration Process
    try {
      Snapshot(sourceETLDB, migrateRootPath, "full",pipeline, tablesToExclude, cloneLevel, "Migration")
    }
    catch{
      case e: Throwable =>
        val failMsg = PipelineFunctions.appendStackStrace(e,"Unable to proceed with Migration  Process")
        logger.log(Level.ERROR, failMsg)
        throw e
    }

    //Step 02 - Update Config with the latest Storage Prefix
    new Migration(sourceETLDB,migrateRootPath,configPath).updateConfig()

    //Step 03 - Drop Source ETLDatabase and Consumer Database after Migration
    spark.sql(s"DROP DATABASE ${etlDatabase} CASCADE")
    spark.sql(s"DROP DATABASE ${consumerDatabase} CASCADE")

    logger.log(Level.INFO, "Migration Completed. Please delete the external data from storage for all source etl database tables")
  }
}


