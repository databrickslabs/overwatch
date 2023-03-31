package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.utils.OverwatchScope._
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import scala.reflect.runtime.{universe => ru}

class InitializerFunctionsUCE(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
  extends InitializerFunctions (config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
    with SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  override private[overwatch] def dataTargetIsValid(dataTarget: DataTarget): Boolean = {
    val dbName = dataTarget.databaseName.getOrElse("overwatch").split("\\.").last
    val rawDBLocation = dataTarget.databaseLocation.getOrElse(s"/user/hive/warehouse/${dbName}.db")
    val dbLocation = PipelineFunctions.cleansePathURI(rawDBLocation)
    val rawETLDataLocation = dataTarget.etlDataPathPrefix.getOrElse(dbLocation)
    val etlDataLocation = PipelineFunctions.cleansePathURI(rawETLDataLocation)
    val etlCatalogName = dataTarget.databaseName.get.split("\\.").head
    var switch = true


    spark.sessionState.catalogManager.setCurrentCatalog(etlCatalogName)
    if (spark.catalog.databaseExists(dbName)) {
      val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(dbName)
      val dbProperties = dbMeta.properties

      val existingDBLocation = spark.sql(s"describe database ${dbName}")
        .where("database_description_item ='RootLocation'")
        .select("database_description_value").collect.map(_.getString(0)).head.toString
      //        .map(f=>f.getString(0)).collect.head.toString

      if (existingDBLocation != dbLocation) {
        switch = false
        throw new BadConfigException(s"The DB: $dbName exists " +
          s"at location $existingDBLocation which is different than the location entered in the config. Ensure " +
          s"the DBName is unique and the locations match. The location must be a fully qualified URI such as " +
          s"dbfs:/...")
      }

      val isOverwatchDB = dbProperties.getOrElse("OVERWATCHDB", "FALSE") == "TRUE"
      val tableList = spark.catalog.listTables(dbName)
      if (!isOverwatchDB && !tableList.isEmpty) {
        switch = false
        throw new BadConfigException(s"The Database: $dbName was not created by overwatch. Specify a " +
          s"database name that does not exist or was created by Overwatch.")
      }
    } else { // Database does not exist
      if (!Helpers.pathExists(dbLocation)) { // db path does not already exist -- valid
        logger.log(Level.INFO, s"Target location " +
          s"is valid: will create database: $dbName at location: ${dbLocation}")
      } else { // db does not exist AND path already exists
        switch = false
        throw new BadConfigException(
          s"""The target database location: ${dbLocation}
          already exists. Please specify a path that doesn't yet exist. If attempting to launch Overwatch on a secondary
          workspace, please choose a unique location for the database on this workspace and use the "etlDataPathPrefix"
          to reference the shared physical data location.""".stripMargin)
      }
    }

    if (Helpers.pathExists(etlDataLocation)) println(s"\n\nWARNING!! The ETL Data Prefix exists. Verify that only " +
      s"Overwatch data exists in this path.")

    // todo - refactor away duplicity
    /**
     * Many of the validation above are required for the consumer DB but the consumer DB will only contain
     * views. It's important that the basic db checks are completed but the checks don't need to be as extensive
     * since there's no chance of data corruption given only creating views. This section needs to be refactored
     * to remove duplicity while still enabling control between which checks are done for which DataTarget.
     */
    val consumerDBName = dataTarget.consumerDatabaseName.getOrElse(dbName).split("\\.").last
    val rawConsumerDBLocation = dataTarget.consumerDatabaseLocation.getOrElse(s"/user/hive/warehouse/${consumerDBName}.db")
    val consumerDBLocation = PipelineFunctions.cleansePathURI(rawConsumerDBLocation)
    val consumerCatalogName = dataTarget.consumerDatabaseName.getOrElse(dbName).split("\\.").head
    spark.sessionState.catalogManager.setCurrentCatalog(consumerCatalogName)
    if (consumerDBName != dbName) { // separate consumer db
      if (spark.catalog.databaseExists(consumerDBName)) {
        val consumerDBMeta = spark.sessionState.catalog.getDatabaseMetadata(consumerDBName)

        val existingConsumerDBLocation = spark.sql(s"describe database ${consumerDBName}")
          .where("database_description_item ='RootLocation'")
          .select("database_description_value").collect.map(_.getString(0)).head.toString

        if (existingConsumerDBLocation != consumerDBLocation) { // separated consumer DB but same location FAIL
          switch = false
          throw new BadConfigException(s"The Consumer DB: $consumerDBName exists" +
            s"at location $existingConsumerDBLocation which is different than the location entered in the config. Ensure" +
            s"the DBName is unique and the locations match. The location must be a fully qualified URI such as " +
            s"dbfs:/...")
        }
      } else { // consumer DB is different from ETL DB AND db does not exist
        if (!Helpers.pathExists(consumerDBLocation)) { // consumer db path is empty
          logger.log(Level.INFO, s"Consumer DB location " +
            s"is valid: will create database: $consumerDBName at location: ${consumerDBLocation}")
        } else {
          switch = false
          throw new BadConfigException(
            s"""The consumer database location: ${dbLocation}
          already exists. Please specify a path that doesn't yet exist. If attempting to launch Overwatch on a secondary
          workspace, please choose a unique location for the database on this workspace.""".stripMargin)
        }
      }

      if (consumerDBLocation == dbLocation && consumerDBName != dbName) { // separate db AND same location ERROR
        switch = false
        throw new BadConfigException("Consumer DB Name cannot differ from ETL DB Name while having the same location.")
      }
    } else { // same consumer db as etl db
      if (consumerDBName == dbName && consumerDBLocation != dbLocation) { // same db AND DIFFERENT location ERROR
        switch = false
        throw new BadConfigException("Consumer DB cannot match ETL DB Name while having different locations.")
      }
    }
    spark.sessionState.catalogManager.setCurrentCatalog(etlCatalogName)
    switch
  }
    /**
     * validate and set the DataTarget for Overwatch
     * @param dataTarget OW DataTarget
     */
    override def validateAndSetDataTarget(dataTarget: DataTarget): Unit = {
      // Validate data Target
      // todo UC enablement

      val etlCatalogName = dataTarget.databaseName.get.split("\\.").head
      spark.sessionState.catalogManager.setCurrentCatalog(etlCatalogName)
      if (!disableValidations) dataTargetIsValid(dataTarget)

      // If data target is valid get db name and location and set it
      val dbName = dataTarget.databaseName.get.split("\\.").last
      val dbLocation = dataTarget.databaseLocation.getOrElse(s"dbfs:/user/hive/warehouse/${dbName}.db")
      val dataLocation = dataTarget.etlDataPathPrefix.getOrElse(dbLocation)


      val consumerDBName = dataTarget.consumerDatabaseName.getOrElse(dbName).split("\\.").last
      val consumerDBLocation = dataTarget.consumerDatabaseLocation.getOrElse(s"/user/hive/warehouse/${consumerDBName}.db")
      val consumerCatalogName = dataTarget.consumerDatabaseName.get.split("\\.").head

      config.setDatabaseNameAndLoc(dbName, dbLocation, dataLocation)
      config.setConsumerDatabaseNameandLoc(consumerDBName, consumerDBLocation)
      config.setCatalogName(etlCatalogName, consumerCatalogName)
    }


    /**
     * Initialize the "Database" object
     * If creating the database special properties will be created to allow overwatch to identify that the db was
     * created through this process. Additionally, the schema version will be noded. This allows for upgrades based on
     * version of Overwatch being executed.
     *
     * @return
     */
    override def initializeDatabase(): Database = {
      // TODO -- Add metadata table
      // TODO -- refactor and clean up duplicity
      val dbMeta = if (isSnap) {
        logger.log(Level.INFO, "Initializing Snap Database")
        "OVERWATCHDB='TRUE',SNAPDB='TRUE'"
      } else {
        logger.log(Level.INFO, "Initializing ETL Database")
        "OVERWATCHDB='TRUE'"
      }
      if (!spark.catalog.databaseExists(s"${config.etlCatalogName}.${config.databaseName}")) {
        logger.log(Level.INFO, s"Database ${config.databaseName} not found, at " +
          s"${config.databaseLocation}.")
        val createDBIfNotExists = s"create database if not exists ${config.etlCatalogName}.${config.databaseName} managed location '" +
            s"${config.databaseLocation}' WITH DBPROPERTIES ($dbMeta,SCHEMA=${config.overwatchSchemaVersion})"

        throw new BadConfigException(s"etlDatabase - ${config.databaseName} not found in catalog ${config.etlCatalogName}. " +
          s"Please create etlDatabase before executing the Overwatch job with this script - ${createDBIfNotExists}")
      } else {
        // TODO -- get schema version of each table and perform upgrade if necessary
        val alterDbPropertiesStatement = s"alter database ${config.databaseName} set DBPROPERTIES ($dbMeta,'SCHEMA'=${config.overwatchSchemaVersion})"
        spark.sql(alterDbPropertiesStatement)
        logger.log(Level.INFO, s"Database ${config.databaseName} already exists, using append mode.")
      }

      // Create consumer database if one is configured
      if (config.consumerDatabaseName != config.databaseName) {
        logger.log(Level.INFO, "Initializing Consumer Database")
        if (!spark.catalog.databaseExists(s"${config.consumerCatalogName}.${config.consumerDatabaseName}")) {
          val createConsumerDBSTMT = s"create database if not exists ${config.consumerCatalogName}.${config.consumerDatabaseName} " +
            s"managed location '${config.consumerDatabaseLocation}'"

          throw new BadConfigException(s"consumerDatabase - ${config.consumerDatabaseName} not found in catalog - ${config.consumerCatalogName}." +
            s" Please create consumerDatabase before executing the Overwatch job with this script - ${createConsumerDBSTMT}")
        }
      }

      Database(config)
    }

}

object InitializerFunctionsUCE{
  def apply(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean): InitializerFunctions= {
    new InitializerFunctionsUCE(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
  }
}