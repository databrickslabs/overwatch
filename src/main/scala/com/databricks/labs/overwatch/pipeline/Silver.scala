package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.SparkSessionWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import com.databricks.labs.overwatch.utils.Helpers._
import com.databricks.labs.overwatch.utils.SchemaTools

class Silver extends Pipeline with Transforms with SparkSessionWrapper{
  import spark.implicits._

  def run(): Boolean = {

    true
  }

  def buildMasterEvents(): Unit = {


  }

}

object Silver {
  def apply(workspace: Workspace): Silver = new Silver()
    .setWorkspace(workspace).setDatabase(workspace.database)

}
