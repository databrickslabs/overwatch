package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.utils.OverwatchScope._
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SerializableConfiguration

class InitializerFunctionsUCE(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
  extends InitializerFunctions (config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
  with SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   *
   * @param dataTarget data target as parsed into OverwatchParams
   * @throws java.lang.IllegalArgumentException
   *  @return
   */
  @throws(classOf[IllegalArgumentException])
  private[overwatch] override def dataTargetIsValid(dataTarget: DataTarget): Boolean = {
    val dbName = dataTarget.databaseName.getOrElse("overwatch")
    val rawDBLocation = dataTarget.databaseLocation.getOrElse(s"/user/hive/warehouse/${dbName}.db")
    val catalogName = dataTarget.catalogName.get
    val dbLocation = PipelineFunctions.cleansePathURI(rawDBLocation)
    val rawETLDataLocation = dataTarget.etlDataPathPrefix.getOrElse(dbLocation)
    val etlDataLocation = PipelineFunctions.cleansePathURI(rawETLDataLocation)
    var switch = true
    if (spark.catalog.databaseExists(s"$catalogName.$dbName")) {
      spark.sql(s"use catalog ${catalogName}")
      val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(dbName)
      val dbProperties = dbMeta.properties
      val existingDBLocation = dbMeta.locationUri.toString
      if (existingDBLocation != dbLocation) {
        switch = false
        throw new BadConfigException(s"The DB: $dbName exists " +
          s"at location $existingDBLocation which is different than the location entered in the config. Ensure " +
          s"the DBName is unique and the locations match. The location must be a fully qualified URI such as " +
          s"dbfs:/...")
      }

      val isOverwatchDB = dbProperties.getOrElse("OVERWATCHDB", "FALSE") == "TRUE"
      if (!isOverwatchDB) {
        switch = false
        throw new BadConfigException(s"The Database: $dbName was not created by overwatch. Specify a " +
          s"database name that does not exist or was created by Overwatch.")
      }
    } else { // Database does not exist
      if (!Helpers.extLocationExists(dbLocation)) { // db path does not already exist -- valid
        println("inside if.....")
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

    if (Helpers.extLocationExists(etlDataLocation)) println(s"\n\nWARNING!! The ETL Data Prefix exists. Verify that only " +
        s"Overwatch data exists in this path.")

    // todo - refactor away duplicity
    /**
     * Many of the validation above are required for the consumer DB but the consumer DB will only contain
     * views. It's important that the basic db checks are completed but the checks don't need to be as extensive
     * since there's no chance of data corruption given only creating views. This section needs to be refactored
     * to remove duplicity while still enabling control between which checks are done for which DataTarget.
     */
    val consumerDBName = dataTarget.consumerDatabaseName.getOrElse(dbName)
    val rawConsumerDBLocation = dataTarget.consumerDatabaseLocation.getOrElse(s"/user/hive/warehouse/${consumerDBName}.db")
    val consumerDBLocation = PipelineFunctions.cleansePathURI(rawConsumerDBLocation)
    if (consumerDBName != dbName) { // separate consumer db
      if (spark.catalog.databaseExists(s"${catalogName}.${consumerDBName}")) {
        spark.sql(s"use catalog ${catalogName}")
        val consumerDBMeta = spark.sessionState.catalog.getDatabaseMetadata(consumerDBName)
        val existingConsumerDBLocation = consumerDBMeta.locationUri.toString
        if (existingConsumerDBLocation != consumerDBLocation) { // separated consumer DB but same location FAIL
          switch = false
          throw new BadConfigException(s"The Consumer DB: $consumerDBName exists" +
            s"at location $existingConsumerDBLocation which is different than the location entered in the config. Ensure" +
            s"the DBName is unique and the locations match. The location must be a fully qualified URI such as " +
            s"dbfs:/...")
        }
      } else { // consumer DB is different from ETL DB AND db does not exist
        if (!Helpers.extLocationExists(consumerDBLocation)) { // consumer db path is empty
          logger.log(Level.INFO, s"Consumer DB location " +
            s"is valid: will create database: $consumerDBName at location: ${consumerDBLocation}")
        }
         else {
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
    switch
  }

  override def validateAndSetDataTarget(dataTarget: DataTarget): Unit = {
    // Validate data Target
    // todo UC enablement
    if (!disableValidations) dataTargetIsValid(dataTarget)

    // If data target is valid get db name and location and set it
    val dbName = dataTarget.databaseName.get
    val dbLocation = dataTarget.databaseLocation.getOrElse(s"dbfs:/user/hive/warehouse/${dbName}.db")
    val dataLocation = dataTarget.etlDataPathPrefix.getOrElse(dbLocation)
    val catalogName = dataTarget.catalogName.get

    val consumerDBName = dataTarget.consumerDatabaseName.getOrElse(dbName)
    val consumerDBLocation = dataTarget.consumerDatabaseLocation.getOrElse(s"/user/hive/warehouse/${consumerDBName}.db")

    config.setDatabaseNameAndLoc(dbName, dbLocation, dataLocation)
    config.setConsumerDatabaseNameandLoc(consumerDBName, consumerDBLocation)
    config.setCatalogName(catalogName)
  }

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
    println(s"catalogName --------- ${config.catalogName}")
    if (!spark.catalog.databaseExists(s"${config.catalogName}.${config.databaseName}")) {
      logger.log(Level.INFO, s"Database ${config.databaseName} not found, creating it at " +
        s"${config.databaseLocation}.")
      val createDBIfNotExists =
        s"create database if not exists ${config.databaseName} managed location '" +
          s"${config.databaseLocation}' WITH DBPROPERTIES ($dbMeta,SCHEMA=${config.overwatchSchemaVersion})"

      spark.sql(s"use catalog ${config.catalogName}")
      spark.sql(createDBIfNotExists)
      logger.log(Level.INFO, s"Successfully created database. $createDBIfNotExists")
    } else {
      // TODO -- get schema version of each table and perform upgrade if necessary
      logger.log(Level.INFO, s"Database ${config.databaseName} already exists, using append mode.")
    }

    // Create consumer database if one is configured
    if (config.consumerDatabaseName != config.databaseName) {
      logger.log(Level.INFO, "Initializing Consumer Database")
      if (!spark.catalog.databaseExists(s"${config.catalogName}.${config.consumerDatabaseName}")) {
        val createConsumerDBSTMT = s"create database if not exists ${config.consumerDatabaseName} " +
          s"managed location '${config.consumerDatabaseLocation}'"
        spark.sql(s"use catalog ${config.catalogName}")
        spark.sql(createConsumerDBSTMT)
        logger.log(Level.INFO, s"Successfully created database. $createConsumerDBSTMT")
      }
    }

    Database(config)
  }

  override def prepTempWorkingDir(tempPath: String): String = {
    val storagePrefix = config.etlDataPathPrefix.split("/").dropRight(1).mkString("/")
    val defaultTempPath = s"$storagePrefix/tempworkingdir/${config.organizationId}"

    if (disableValidations) {
      // if not validated -- don't drop data but do append millis in case data dir is not empty
      // prevent dropping data on accidental registration of temp dir
      val currentMillis = System.currentTimeMillis()
      if (tempPath == "") s"$defaultTempPath/$currentMillis" else s"$tempPath/$currentMillis"
    } else {
      val workspaceTempWorkingDir = if (tempPath == "") { // default null string
        defaultTempPath
      }
      else if (tempPath.split("/").takeRight(1).headOption.getOrElse("") == config.organizationId) { // if user defined value already ends in orgid don't append it
        tempPath
      } else { // if user configured value doesn't end with org id append it
        s"$tempPath/${config.organizationId}"
      }
      if (Helpers.extLocationExists(workspaceTempWorkingDir)) { // if temp path exists clean i
        dbutils.fs.rm(workspaceTempWorkingDir, recurse= true)
      }
      // ensure path exists at init
      dbutils.fs.mkdirs(workspaceTempWorkingDir)
      workspaceTempWorkingDir
    }
  }

}

object InitializerFunctionsUCE{
  def apply(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean): InitializerFunctions= {
    new InitializerFunctionsUCE(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
  }
}