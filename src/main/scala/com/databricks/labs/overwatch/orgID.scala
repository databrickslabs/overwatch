package com.databricks.labs.overwatch
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.utils._

object orgID extends SparkSessionWrapper {
  def orgID(): Unit = {
    val clusterOwnerOrgID = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    val apiUrl = dbutils.notebook.getContext.apiUrl
    println("apiUrl is ", apiUrl)
    println("clusterOwnerOrgID is ", clusterOwnerOrgID)
    println("Splitted ApiURL is ", dbutils.notebook.getContext.apiUrl.get.split("\\.")(0).split("/").last)
  }

  def main(args: Array[String]) {
    // prints Hello World
    println("ORGID")

  }
}
