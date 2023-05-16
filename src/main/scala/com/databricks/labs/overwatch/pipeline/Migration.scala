package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.MultiWorkspaceDeployment
import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import com.databricks.labs.overwatch.utils.Helpers.removeTrailingSlashes
import com.databricks.labs.overwatch.MultiWorkspaceDeployment._
import org.apache.spark.sql.functions._

import java.io.FileNotFoundException

/**
 *
 * @param _sourceETLDB        ETL Database for Souce from where Migration need to be done.
 * @param _targetPrefix       Target Path for where Migration would be done
 * @param _configPath         path for Configuration file for which migration need to be performed.
 */

class Migration(_sourceETLDB: String, _targetPrefix: String, _configPath: String) {

  private val storage_prefix = removeTrailingSlashes(_targetPrefix)
  private val sourceETLDB = _sourceETLDB
  private val configPath = _configPath


  private[overwatch] def updateConfig(): Unit = {
    if (configPath.toLowerCase().endsWith(".csv")) {
      println(s"Config source: csv path ${configPath}")
      val tempConfigPath = (configPath.split("/").dropRight(1) :+ "tempConfig.csv").mkString("/")
      spark.read.format("csv")
        .option("header", "true")
        .option("ignoreLeadingWhiteSpace", true)
        .option("ignoreTrailingWhiteSpace", true)
        .load(configPath)
        .withColumn("storage_prefix", when(col("etl_database_name") === lit(sourceETLDB), lit(storage_prefix)).otherwise(col("storage_prefix")))
        .coalesce(1)
        .write
        .format("csv")
        .option("header", "true")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(tempConfigPath)
      val filePath = dbutils.fs.ls(tempConfigPath).last.path
      dbutils.fs.cp(filePath, configPath, true)
      dbutils.fs.rm(tempConfigPath, true)
    } else {
      val configUpdateStatement =
        s"""
      update delta.`$configPath`
      set
        storage_prefix = '$storage_prefix'
      Where etl_database_name = '$sourceETLDB'
      """
      spark.sql(configUpdateStatement)
    }
  }
}

object Migration extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Create a backup of the Overwatch datasets
   *
   * @param arg(0)        Source Database name or ETlDataPathPrefix name.
   * @param arg(2)        Target path to where migration need to be performed.
   * @param arg(2)        Configuration Path where the config file for the source is present.
   * @return
   */

  def main(args: Array[String]): Unit = {

    val sourceETLDB = args(0)
    val migrateRootPath = args(1)
    val configPath = args(2)
    val tablesToExclude = args.lift(3).getOrElse("")
    val cloneLevel = "Deep"

    val configDF = new MultiWorkspaceDeployment().generateBaseConfig(configPath).filter(col("etl_database_name") === sourceETLDB)
    if (configDF.select("storage_prefix").distinct().count() != 1){
      throw new BadConfigException("Migration is not possible where multiple different storage_prefix present for etl_database_name")
    }
    val pipeline = "Bronze,Silver,Gold"
    val etlDatabase = configDF.select("etl_database_name").distinct().collect()(0)(0)
    val consumerDatabase = configDF.select("consumer_database_name").distinct().collect()(0)(0)


    // Step 01 - Start Migration Process
    Snapshot.main(Array(sourceETLDB,migrateRootPath,pipeline,"full",tablesToExclude,cloneLevel,"Migration"))

    //Step 02 - Drop Source ETLDatabase and Consumer Database after Migration
    spark.sql(s"DROP DATABASE ${etlDatabase} CASCADE")
    spark.sql(s"DROP DATABASE ${consumerDatabase} CASCADE")

    //Step 03 - Update Config with the latest Storage Prefix
    new Migration(sourceETLDB,migrateRootPath,configPath).updateConfig()
    println("Migration Completed")
  }
}


