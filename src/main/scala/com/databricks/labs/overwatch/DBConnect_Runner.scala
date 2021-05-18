package com.databricks.labs.overwatch

import com.databricks.backend.common.rpc.CommandContext
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.pipeline.InitializerFunctions.getClass
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.overwatch.validation._
import com.databricks.labs.overwatch.pipeline._

import scala.collection.parallel.ForkJoinTaskSupport
import java.util.concurrent.ForkJoinPool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

object DBConnect_Runner extends SparkSessionWrapper with DBConnect_Helpers {

  private def showReport(df: DataFrame): Unit = {
    df.show(200, false)
  }
  def main(args: Array[String]): Unit = {

    spark.conf.set("spark.databricks.service.client.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    sc.addJar("C:\\Dev\\git\\Databricks--Overwatch\\target\\scala-2.12\\overwatch-assembly-0.4.2.jar")
    envInit("WARN")

    val prodWorkspace = getWorkspace()

    val snapDBName = "tomes_kitana_snapv1"
//    spark.sql(s"drop database if exists $snapDBName cascade")
//    val kitana = Kitana(snapDBName, prodWorkspace, parallelism = Some(parallelism))

//    spark.conf.set("overwatch.permit.db.destruction", snapDBName)
//    val snapReport = kitana.executeBronzeSnapshot()
//    val snapReport = kitana.refreshSnapshot(completeRefresh = true)
//    showReport(snapReport.toDF())

    val subsetWorkspace = getWorkspace("audit, clusters".split(", "))
    val kitanaSubset = Kitana(snapDBName, subsetWorkspace, parallelism = Some(parallelism))
    kitanaSubset.executeSilverGoldRebuild(primordialPadding = 2, Some(2))

//    val validationReport = kitana.validateEquality(
//      primordialPadding = 3,
//      maxDays = Some(4),
//      tol = 0.05
//    )
//    showReport(validationReport.toDF())

//    val schemaValidationReport = kitanaSubset.validateSchemas()
//    schemaValidationReport.drop("actualColumns").show(200, false)

//    val keyReport = kitana.validateKeys()
//    keyReport.show(200, false)

//    val (dupReport, dupsByTarget) = kitana.identifyDups(dfsByKeysOnly = true)
//    showReport(dupReport.toDF())

//    spark.conf.set("overwatch.permit.db.destruction", snapDBName)
//    kitana.fullResetSnapDB()

  }
}
