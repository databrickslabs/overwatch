package com.databricks.labs.overwatch.pipeline

//import Initializer.{InitializerFunctionsTemplate, InitializerFunctionsUCE}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}

class InitializerFunctionsUCM(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
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
    println("since this is a uc deployment for managed tables no need to validate external location")
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
    if (!config.catalogName.isEmpty){
      val createDBIfNotExists =
        s"create database if not exists ${config.catalogName}.${config.databaseName} " +
          s"WITH DBPROPERTIES ($dbMeta,SCHEMA=${config.overwatchSchemaVersion})"
      spark.sql(s"use catalog ${config.catalogName}")
      println(createDBIfNotExists)
      spark.sql(createDBIfNotExists)
      logger.log(Level.INFO, s"Successfully created database. $createDBIfNotExists")
    }
    else if (!spark.catalog.databaseExists(config.databaseName)) {
      logger.log(Level.INFO, s"Database ${config.databaseName} not found, creating it at " +
        s"${config.databaseLocation}.")
      val createDBIfNotExists =
        s"create database if not exists ${config.databaseName} " +
          s"WITH DBPROPERTIES ($dbMeta,SCHEMA=${config.overwatchSchemaVersion})"
      spark.sql(createDBIfNotExists)
      logger.log(Level.INFO, s"Successfully created database. $createDBIfNotExists")
    } else {
      // TODO -- get schema version of each table and perform upgrade if necessary
      logger.log(Level.INFO, s"Database ${config.databaseName} already exists, using append mode.")
    }

    // Create consumer database if one is configured and catalog name is provided
    if (config.consumerDatabaseName != config.databaseName && config.catalogName.nonEmpty) {
      logger.log(Level.INFO, "Initializing Consumer Database")
      if (!spark.catalog.databaseExists(config.consumerDatabaseName)) {
        val createConsumerDBSTMT = s"create database if not exists ${config.catalogName}.${config.consumerDatabaseName} "
        spark.sql(s"use catalog ${config.catalogName}")
        spark.sql(createConsumerDBSTMT)
        logger.log(Level.INFO, s"Successfully created database. $createConsumerDBSTMT")
      }
    }

    // Create consumer database if one is configured
    if (config.consumerDatabaseName != config.databaseName) {
      logger.log(Level.INFO, "Initializing Consumer Database")
      if (!spark.catalog.databaseExists(config.consumerDatabaseName)) {
        //          val createConsumerDBSTMT = s"create database if not exists ${config.consumerDatabaseName} " +
        //            s"location '${config.consumerDatabaseLocation}'"
        val createConsumerDBSTMT = s"create database if not exists ${config.catalogName}.${config.consumerDatabaseName} "
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

object InitializerFunctionsUCM{
  def apply(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean): InitializerFunctions= {
    new InitializerFunctionsUCM(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
  }
}