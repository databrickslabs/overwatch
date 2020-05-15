package com.databricks.labs.overwatch.pipeline

import java.io.StringWriter

import com.databricks.labs.overwatch.env.{Database, Workspace}
import com.databricks.labs.overwatch.utils.{Config, ModuleStatusReport, SparkSessionWrapper}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

class Silver(_workspace: Workspace, _database: Database, _config: Config)
  extends Pipeline(_workspace, _database, _config)
  with SilverTransforms with SparkSessionWrapper{
  import spark.implicits._

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val sw = new StringWriter

  //TODO - FIX
  setTransformDatabaseName(config.databaseName)



//  def appendTasks: ModuleStatusReport = {
//    val startTime = System.currentTimeMillis()
//    val moduleID = 2001
//    val moduleName = "tasks_silver"
//    val status: String = try {
  // TODO -- append Should have ability to window since until snap over new keys
//      val df =
//    }
//  }

  def run(): Boolean = {

    true
  }


}

object Silver {
  def apply(workspace: Workspace): Silver = new Silver(workspace, workspace.database, workspace.getConfig)
//    .setWorkspace(workspace).setDatabase(workspace.database)

}
