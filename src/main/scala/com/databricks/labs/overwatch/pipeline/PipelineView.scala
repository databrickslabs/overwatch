package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper, Helpers}
import org.apache.log4j.{Level, Logger}

case class PipelineView(name: String,
                        dataSource: PipelineTable,
                        config: Config,
                        partitionMapOverrides: Map[String, String] = Map(),
                        dbTargetOverride: Option[String] = None
                       ) extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)
//  private val dbTarget = dbTargetOverride.getOrElse(config.consumerDatabaseName)
  private val dbTarget = if(config.deploymentType!="default")
    s"${config.consumerCatalogName}.${dbTargetOverride.getOrElse(config.consumerDatabaseName)}"
  else dbTargetOverride.getOrElse(config.consumerDatabaseName)

  def publish(colDefinition: String, sorted: Boolean = false, reverse: Boolean = false,workspacesAllowed: Array[String] = Array()): Unit = {

    if (spark.catalog.tableExists(s"${config.etlCatalogName}.${config.databaseName}.${dataSource.name}")) {
      createView(colDefinition, sorted, reverse,workspacesAllowed)
    } else {
      val dataSourceName = dataSource.name.toLowerCase()
      val path = s"${config.etlDataPathPrefix}/${dataSourceName}"
      if (Helpers.pathExists(path)) {
        val query = s"CREATE TABLE ${config.etlCatalogName}.${config.databaseName}.${dataSource.name} USING DELTA LOCATION '${path}'"
        val queryStatement = query.toString()
        try {
          setCurrentCatalog(spark, config.etlCatalogName)
          spark.sql(queryStatement)
          createView(colDefinition, sorted, reverse,workspacesAllowed)
        } catch {
          case e: Throwable =>
            println(s"GOLD VIEW: CREATE VIEW FAILED: Cannot create view: ${dataSource.tableFullName} --> ${e.getMessage}")
        }
      } else {
        val msgLog = s"GOLD VIEW: CREATE VIEW FAILED: Source path: ${path}  does not exist"
        logger.log(Level.INFO, msgLog)
        if (config.debugFlag) println(msgLog)
      }
    }
  }

  private def createView(colDefinition: String, sorted: Boolean = false, reverse: Boolean = false, workspacesAllowed: Array[String] = Array()): Unit = {

    if (dataSource.exists) {
      val pubStatementSB = new StringBuilder("create or replace view ")
      val dataSourceName = dataSource.name.toLowerCase()
      pubStatementSB.append(s"${dbTarget}.${name} as select ${colDefinition} from ${config.etlCatalogName}.${config.databaseName}.${dataSource.name}")
      // link partition columns
      if (dataSource.partitionBy.nonEmpty) {
        val partMap: Map[String, String] = if (partitionMapOverrides.isEmpty) {
          dataSource.partitionBy.map({ case (pCol) => (pCol, pCol) }).toMap
        } else {
          partitionMapOverrides
        }
        pubStatementSB.append(s" where ${partMap.head._1} = ${config.etlCatalogName}.${config.databaseName}.${dataSource.name}.${partMap.head._2} ")
        if (partMap.keys.toArray.length > 1) {
          partMap.tail.foreach(pCol => {
            pubStatementSB.append(s"and ${pCol._1} = ${config.etlCatalogName}.${config.databaseName}.${dataSource.name}.${pCol._2} ")
          })
        }
        if (workspacesAllowed.nonEmpty){
          val workspacesAllowedString = workspacesAllowed.mkString("'", "','", "'")
          pubStatementSB.append(s" and organization_id in (${workspacesAllowedString})")
        }
        if (sorted) {
          val orderDef = if (reverse) {
            dataSource.incrementalColumns.map(c => s"$c desc").mkString(", ")
          } else {
            dataSource.incrementalColumns.mkString(", ")
          }
          pubStatementSB.append(s"order by $orderDef ")
        }
      }
      val pubStatement = pubStatementSB.toString()

      val msgLog = s"GOLD VIEW: CREATE: Statement --> $pubStatement\n\n"
      logger.log(Level.INFO, msgLog)
      if (config.debugFlag) println(msgLog)
      try {
        setCurrentCatalog(spark,config.consumerCatalogName)
        spark.sql(pubStatement)
      } catch {
        case e: Throwable =>
          println(s"GOLD VIEW: CREATE VIEW FAILED: Cannot create view: ${dataSource.tableFullName} --> ${e.getMessage}")
      }
    } else {
      val msgLog = s"GOLD VIEW: CREATE VIEW FAILED: Source table: ${dataSource.tableFullName} empty or does not exist"
      logger.log(Level.INFO, msgLog)
      if (config.debugFlag) println(msgLog)
    }
  }
}