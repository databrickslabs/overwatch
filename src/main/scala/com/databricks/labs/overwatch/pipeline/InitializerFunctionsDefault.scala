package com.databricks.labs.overwatch.pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Database
import com.databricks.labs.overwatch.utils.OverwatchScope.{OverwatchScope, _}
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SerializableConfiguration

class InitializerFunctionsDefault(config: Config, disableValidations: Boolean, isSnap: Boolean, initDB: Boolean)
  extends Initializer(config, disableValidations)
    with SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

    /**
     * validate and set the DataTarget for Overwatch
     * @param dataTarget OW DataTarget
     */
    override def validateAndSetDataTarget(dataTarget: DataTarget): Unit = {
      // Validate data Target
      // todo UC enablement
      if (!disableValidations) dataTargetIsValid(dataTarget)

      // If data target is valid get db name and location and set it
      val dbName = dataTarget.databaseName.get
      val dbLocation = dataTarget.databaseLocation.getOrElse(s"dbfs:/user/hive/warehouse/${dbName}.db")
      val dataLocation = dataTarget.etlDataPathPrefix.getOrElse(dbLocation)

      val consumerDBName = dataTarget.consumerDatabaseName.getOrElse(dbName)
      val consumerDBLocation = dataTarget.consumerDatabaseLocation.getOrElse(s"/user/hive/warehouse/${consumerDBName}.db")
      val etlCatalogName = "hive_metastore"
      val consumerCatalogName = "hive_metastore"

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
    def initializeDatabase(): Database = {
      // TODO -- Add metadata table
      // TODO -- refactor and clean up duplicity
      if (initDB) {
        val dbMeta = if (isSnap) {
          logger.log(Level.INFO, "Initializing Snap Database")
          "OVERWATCHDB='TRUE',SNAPDB='TRUE'"
        } else {
          logger.log(Level.INFO, "Initializing ETL Database")
          "OVERWATCHDB='TRUE'"
        }
        if (!spark.catalog.databaseExists(config.databaseName)) {
          logger.log(Level.INFO, s"Database ${config.databaseName} not found, creating it at " +
            s"${config.databaseLocation}.")
          val createDBIfNotExists = s"create database if not exists ${config.databaseName} location '" +
            s"${config.databaseLocation}' WITH DBPROPERTIES ($dbMeta,SCHEMA=${config.overwatchSchemaVersion})"

          spark.sql(createDBIfNotExists)
          logger.log(Level.INFO, s"Successfully created database. $createDBIfNotExists")
        } else {
          // TODO -- get schema version of each table and perform upgrade if necessary
          logger.log(Level.INFO, s"Database ${config.databaseName} already exists, using append mode.")
        }

        // Create consumer database if one is configured
        if (config.consumerDatabaseName != config.databaseName) {
          logger.log(Level.INFO, "Initializing Consumer Database")
          if (!spark.catalog.databaseExists(config.consumerDatabaseName)) {
            val createConsumerDBSTMT = s"create database if not exists ${config.consumerDatabaseName} " +
              s"location '${config.consumerDatabaseLocation}'"

            spark.sql(createConsumerDBSTMT)
            logger.log(Level.INFO, s"Successfully created database. $createConsumerDBSTMT")
          }
        }
      }
      Database(config)
    }


}