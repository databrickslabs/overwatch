package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{Config, SparkSessionWrapper}
import org.apache.log4j.{Level, Logger}

case class PipelineView(name: String,
                        dataSource: PipelineTable,
                        config: Config,
                        partitionMapOverrides: Map[String, String] = Map()) extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def publish(colDefinition: String): Unit = {
    val pubStatementSB = new StringBuilder("create or replace view ")
    pubStatementSB.append(s"${config.consumerDatabaseName}.${name} as select ${colDefinition} from ${dataSource.tableFullName} ")

    // link partition columns
    if (dataSource.partitionBy.nonEmpty) {
      val partMap: Map[String, String] = if (partitionMapOverrides.isEmpty) {
        dataSource.partitionBy.map({case (pCol) => (pCol, pCol)}).toMap
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
//    DEPRECATED
//    val pubStatement =
//      s"""
//         |create or replace view ${config.consumerDatabaseName}.${name} as select ${colDefinition}
//         |from ${dataSource.tableFullName}
//         |""".stripMargin

    logger.log(Level.INFO, s"GOLD VIEW: CREATE: Statement --> $pubStatement")
    spark.sql(pubStatement)
  }
}
