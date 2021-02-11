package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.{IncrementalFilter, Module}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, date_add, lit}

object PipelineFunctions {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def addOneTick(ts: Column, dt: DataType = TimestampType): Column = {
    dt match {
      case _: TimestampType =>
        ((ts.cast("double") * 1000 + 1) / 1000).cast("timestamp")
      case _: DateType =>
        date_add(ts, 1)
      case _: DoubleType =>
        ts + 0.001d
      case _: LongType =>
        ts + 1
      case _: IntegerType =>
        ts + 1
      case _ => throw
        new UnsupportedOperationException(s"Cannot add milliseconds to ${dt.typeName}")
    }
  }

  def applyFilters(df: DataFrame, filters: Seq[Column], module: Option[Module] = None): DataFrame = {
    if (module.nonEmpty) {
      val filterLogMessageSB: StringBuilder = new StringBuilder
      filterLogMessageSB.append(s"APPLIED FILTERS:\nMODULE_ID: ${module.get.moduleID}\nMODULE_NAME: ${module.get.moduleName}\nFILTERS:\n")
      filters.map(_.expr).foreach(filterLogMessageSB.append)
      val filterLogMessage = filterLogMessageSB.toString()
      println(filterLogMessage)
      logger.log(Level.INFO, filterLogMessage)
    }
    filters.foldLeft(df) {
      case (rawDF, filter) =>
        rawDF.filter(filter)
    }
  }

  // TODO -- handle complex data types such as structs with format "jobRunTime.startEpochMS"
  def withIncrementalFilters(df: DataFrame, module: Module, filters: Seq[IncrementalFilter], globalFilters: Option[Seq[Column]] = None): DataFrame = {
    val parsedFilters = filters.map(filter => {
      val c = filter.cronColName
      val low = filter.low
      val high = filter.high
      val dt = df.schema.fields.filter(_.name == c).head.dataType
      dt match {
        case _: TimestampType =>
          col(c).between(PipelineFunctions.addOneTick(low), high)
        case _: DateType => {
          col(c).between(PipelineFunctions.addOneTick(low.cast(DateType), DateType), high.cast(DateType))
        }
        case _: LongType =>
          col(c).between(PipelineFunctions.addOneTick(low, LongType), high.cast(LongType))
        case _: IntegerType =>
          col(c).between(PipelineFunctions.addOneTick(low, IntegerType), high.cast(IntegerType))
        case _: DoubleType =>
          col(c).between(PipelineFunctions.addOneTick(low, DoubleType), high.cast(DoubleType))
        case _ =>
          throw new IllegalArgumentException(s"IncreasingID Type: ${dt.typeName} is Not supported")
      }
    })

    val allFilters = parsedFilters ++ globalFilters.getOrElse(Seq())

    applyFilters(df, allFilters, Some(module))

  }

  def isIgnorableException(e: Exception): Boolean = {
    val message = e.getMessage()
    message.contains("Cannot modify the value of a static config")
  }

  def setSparkOverrides(spark: SparkSession, sparkOverrides: Map[String, String],
                        debugFlag: Boolean = false): Unit = {
    sparkOverrides foreach { case (k, v) =>
      try {
        if (debugFlag) {
          val opt = spark.conf.getOption(k)
          if (opt.isEmpty || opt.get != v) {
            println(s"Overriding $k from $opt --> $v")
          }
        }
        spark.conf.set(k, v)
      } catch {
        case e: AnalysisException =>

          if (!isIgnorableException(e)) {
            logger.log(Level.WARN, s"Cannot Set Spark Param: $k", e)
            if (debugFlag)
              println(s"Failed Setting $k", e)
          }
        case e: Throwable =>
          if (debugFlag)
            println(s"Failed Setting $k", e)
          logger.log(Level.WARN, s"Failed trying to set $k", e)
      }
    }
  }
}
