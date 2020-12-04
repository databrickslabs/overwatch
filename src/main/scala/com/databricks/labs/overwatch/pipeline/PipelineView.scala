package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Config
import org.apache.log4j.{Level, Logger}

case class PipelineView(name: String,
                        dataSource: PipelineTable,
                        config: Config) {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def publish(colDefinition: String): Unit = {
    val pubStatement =
      s"""
         |create or replace view ${config.databaseName}.${name} as select ${colDefinition}
         | from ${dataSource.tableFullName}
         |""".stripMargin
  }
}
