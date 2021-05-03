package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}

case class PipelineView(name: String,
                        dataSource: PipelineTable,
                        config: Config,
                        partitionMapOverrides: Map[String, String] = Map(),
                        dbTargetOverride: Option[String] = None
                       ) extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)
  private val dbTarget = dbTargetOverride.getOrElse(config.consumerDatabaseName)

  def publish(colDefinition: String): Unit = {
    if (dataSource.exists) {
      val pubStatementSB = new StringBuilder("create or replace view ")
      pubStatementSB.append(s"${dbTarget}.${name} as select ${colDefinition} from ${dataSource.tableFullName} ")

      // link partition columns
      if (dataSource.partitionBy.nonEmpty) {
        val partMap: Map[String, String] = if (partitionMapOverrides.isEmpty) {
          dataSource.partitionBy.map({ case (pCol) => (pCol, pCol) }).toMap
        } else {
          partitionMapOverrides
        }
        pubStatementSB.append(s"where ${partMap.head._1} = ${dataSource.name}.${partMap.head._2} ")
        if (partMap.keys.toArray.length > 1) {
          partMap.tail.foreach(pCol => {
            pubStatementSB.append(s"and ${pCol._1} = ${dataSource.name}.${pCol._2} ")
          })
        }
      }
      val pubStatement = pubStatementSB.toString()

      val msgLog = s"GOLD VIEW: CREATE: Statement --> $pubStatement"
      logger.log(Level.INFO, msgLog)
      if (config.debugFlag) println(msgLog)
      try {
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
