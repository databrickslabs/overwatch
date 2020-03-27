package com.databricks.labs.overwatch.utils

import java.util.Calendar

import com.databricks.labs.overwatch.utils.GlobalStructures.{Bounds, Result, Rule, RuleDetails}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

import scala.collection.mutable.ArrayBuffer

class Rules extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  import spark.implicits._
  private var _df: Any = _
  private var _isGrouped: Boolean = _
  private val rulesReport = ArrayBuffer[Result]()
  private val metaData = new MetadataBuilder()
  private val rules = ArrayBuffer[Rule]()
  private val finalBys = ArrayBuffer[Array[Column]]()

  def setDF(value: Any): this.type = {
    _df = value
    this
  }
  def setIsGrouped(value: Boolean): this.type = {
    _isGrouped = value
    this
  }
  def getDF: Any = _df
  def getMetadata: Metadata = metaData.build()

  def validate: (DataFrame, Boolean) = {
    applyValidation
    val reportDF = rulesReport.toDS.toDF
    (reportDF,
      reportDF.filter('passed === false).count > 0)

//    val wByBys = Window.partitionBy()
//    val colNames = rules.toArray.map(rule => {
//      rule.column.expr.asInstanceOf[NamedExpression].name
//    })
//
//    // Get Unique bys
//    rules.toArray.foreach(rule => {
//      if (!finalBys.toArray.deep.contains(rule.by.toArray.deep)) finalBys.append(rule.by.toArray)
//    })
//
//
//
//
//
//    _df.groupBy(rules(0).by: _*)
//      .agg(
//        rules(0).aggFunc(rules(0).column).alias(rules(0).alias),
//        (rules(0).aggFunc(rules(0).column) < rules(0).boundaries.upper &&
//          rules(0).aggFunc(rules(0).column) > rules(0).boundaries.lower).alias(rules(0).alias + "_passed")
//      )


//    val w = Window.partitionBy('month)
//    // For each by for each rule
//    display(
//      df
//        .withColumn("maxFFMC", max('FFMC).over(w))
//        .withColumn("maxDMC", max('DMC).over(w))
//        .withColumn("maxFFMC_failed", when('maxFFMC < lit(92.0), true).otherwise(false))
//        .withColumn("maxDMC_failed", when('maxDMC < lit(200.0), true).otherwise(false))
//        .groupBy()
//        .agg(
//          collect_set(when('maxFFMC_failed, 'month)).alias("maxFFMC_failed_bys"),
//          collect_set(when('maxFFMC_failed, 'maxFFMC)).alias("maxFFMC_failed_vals"),
//          collect_set(when('maxDMC_failed, 'month)).alias("maxDMC_failed_bys"),
//          collect_set(when('maxDMC_failed, 'maxDMC)).alias("maxDMC_failed_vals")
//        )
//    )

  }

  def add(rule: Rule*): this.type = {
    rules.append(rule: _*)
    this
  }



  private def addMetaData(k: String, Test: String): MetadataBuilder = {
    val msg = s"$Test captured this meta example on ${Calendar.getInstance().getTime.toString}"
    metaData.putString(k, msg)
  }

  def applyValidation: this.type = {

    val actuals = rules.map(rule => rule.aggFunc(rule.column).alias(rule.alias)).toArray
    val results = if (_isGrouped) {
      _df.asInstanceOf[RelationalGroupedDataset].agg(actuals.head, actuals.tail: _*).first()
    } else { _df.asInstanceOf[DataFrame].select(actuals: _*).first() }

    rules.toArray.foreach(rule => {
      val colName = rule.column.expr.asInstanceOf[NamedExpression].name
      val actVal = results.getDouble(results.fieldIndex(rule.alias))
      rulesReport.append(Result(rule.ruleName, rule.alias, rule.boundaries,
          actVal, actVal < rule.boundaries.upper && actVal > rule.boundaries.lower))

    })

//    val colDetails = colsThresholds.map(c => {
//      val funcRaw = aggFunc.apply(c._1).toString()
//      val funcName = funcRaw.substring(0, funcRaw.indexOf("("))
//      ColDetails(ruleName, c._1, c._1.expr.asInstanceOf[NamedExpression].name, s"${funcName}_${c._1}", c._2)
//    })
//
////    val actuals = colDetails.map(details => aggFunc(details.origCol).alias(details.aggName)).toArray
//    val results = _df.select(actuals: _*).first()
//    colDetails.foreach(details => {
//      val actualVal = results.getDouble(results.fieldIndex(details.aggName))
//      rulesReport.append(Result(ruleName, details.aggName, details.bounds,
//        actualVal, actualVal < details.bounds.upper && actualVal > details.bounds.lower))
//    })
    this
  }
//
//
////   use multiple agg funcs to only group once
//  def validationsBy(rules: Array[Rule], by: Column*): (DataFrame, Boolean) = {
//
//    val colDetails = rules.map(rule => {
//      val colName = rule.column.expr.asInstanceOf[NamedExpression].name
//      val funcRaw = rule.aggFunc.apply(rule.column).toString()
//      val funcName = funcRaw.substring(0, funcRaw.indexOf("("))
//      ColDetails2(rule.ruleName, colName, funcName, rule.boundaries)
//    })
//
//
//    val aggs = rules.map(rule => rule.aggFunc(rule.column).alias(rule.alias))
//    val results = _df.groupBy(by: _*).agg(aggs.head, aggs.tail: _*)
//      .rdd.flatMap(row => {
//      colDetails.map(detail => {
//        val actualVal = row.getDouble(row.fieldIndex(detail.aggName))
//        val passed = actualVal < detail.bounds.upper && actualVal > detail.bounds.lower
//        Result(detail.ruleName, detail.aggName, detail.bounds, actualVal, passed)
//      })
//    }).toDS.toDF
////    val passed = results.filter('passed === false).count() == 0
//    (results, false)
//  }
//
}

object Rules {
  def apply(df: DataFrame): Rules = {
    new Rules().setDF(df).setIsGrouped(false)
  }

  def apply(df: RelationalGroupedDataset): Rules = {
    new Rules().setDF(df).setIsGrouped(true)
  }

}
