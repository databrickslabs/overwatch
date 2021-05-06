package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.{Bronze, Pipeline, PipelineTable, Schema}
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.{BadConfigException, Config, DataTarget, PipelineStateException, SparkSessionWrapper, TimeTypes, TimeTypesConstants}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, MapType, StringType, TimestampType}
import org.apache.log4j.{Level, Logger}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDate


trait ValidationUtils extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  protected def getDateFormat: SimpleDateFormat = getDateFormat(None)

  protected def getDateFormat(dtFormatString: Option[String]): SimpleDateFormat = {
    val dtFormatStringFinal = dtFormatString.getOrElse(TimeTypesConstants.dtStringFormat)
    new SimpleDateFormat(dtFormatStringFinal)
  }

  @throws(classOf[BadConfigException])
  private def validateDuration(actualDuration: Int, recommendedDuration: Int = 30, minimumDuration: Int = 3): Unit = {
    val recommendDuration = s"It's strongly recommended to expand this window to at least $recommendedDuration days " +
      s"and even 60 days for best results."

    require(actualDuration >= minimumDuration, s"A test snap of only $actualDuration days is insufficient for " +
      s"a snapshot. $recommendDuration")

    if (actualDuration < recommendedDuration) {
      val shortTestWarnMsg = s"SHORT TEST WINDOW: $actualDuration day test window provided. $recommendDuration"
      logger.log(Level.WARN, shortTestWarnMsg)
      println(shortTestWarnMsg)
    }
  }

  @throws(classOf[BadConfigException])
  private def validateSnapDatabase(pipeline: Pipeline): Unit = {
    val config = pipeline.config
    val dbProperties = spark.sessionState.catalog.getDatabaseMetadata(config.databaseName).properties
    val isVerifiedSnap = dbProperties.getOrElse("SNAPDB", "FALSE") == "TRUE"
    require(isVerifiedSnap, s"The database, ${config.databaseName} was not created by the Overwatch snapshot " +
      s"process. Any target snap database must be created, owned, and maintained through Overwatch validation " +
      s"processes.")

    if (pipeline.BronzeTargets.auditLogsTarget.exists) { // snap db already exists
      throw new BadConfigException(s"A snapshot may only be created once and it must be created by " +
        s"the overwatch snapshot process. If you would like to create another snapshot and overwrite this one " +
        s"please use the 'refreshSnapshot' function in the validation package.")
    }
  }

  @throws(classOf[BadConfigException])
  protected def validateSnapPipeline(
                                      sourceWorkspace: Workspace,
                                      pipeline: Pipeline,
                                      snapFromTime: TimeTypes,
                                      snapUntilTime: TimeTypes
                                    ): Unit = {
    validateSnapDatabase(pipeline)
    val sourceConfig = sourceWorkspace.getConfig
    val sourceBronzePipeline = Bronze(sourceWorkspace, readOnly = true)
    if (sourceBronzePipeline.getPipelineState.isEmpty) {
      throw new PipelineStateException("PIPELINE STATE ERROR: The state of the source cannot be determined.")
    } else {
      val sourceMaxUntilTS = sourceBronzePipeline.getPipelineState.values.toArray.maxBy(_.untilTS).untilTS
      require(sourceMaxUntilTS >= snapUntilTime.asUnixTimeMilli, s"PIPELINE STATE ERROR: The maximum state of " +
        s"any module in the source is < the specified end period of ${snapUntilTime.asDTString}. The snap window " +
        s"must be WITHIN the timeframe of the source data.")
    }

    val sourcePrimordialDate = Pipeline.deriveLocalDate(sourceConfig.primordialDateString.get, getDateFormat(None))
    val snapPrimordialDate = snapFromTime.asLocalDateTime.toLocalDate

    require(snapPrimordialDate.toEpochDay >= sourcePrimordialDate.toEpochDay, s"The specified start date of " +
      s"$snapPrimordialDate is < the source's primordial date of $sourcePrimordialDate. The snap window must be " +
      s"WITHIN the timeframe of the source data.")

    // already validated and confirmed max days == duration
    validateDuration(pipeline.config.maxDays)
  }

  @throws(classOf[PipelineStateException])
  protected def getPrimordialSnapshot(pipeline: Pipeline): LocalDate = {
    val snapAuditState = pipeline.getModuleState(1004)
    require(snapAuditState.nonEmpty, "The state of the bronze snapshot cannot be determine. A bronze snapshot " +
      "must exist prior to executing Silver/Gold recalculations. If you haven't yet run created the snapshot you " +
      "may do so by running Kitana.executeBronzeSnapshot(...)")
    val bronzeSnapPrimordialDateString = snapAuditState.get.primordialDateString.get
    Pipeline.deriveLocalDate(bronzeSnapPrimordialDateString, getDateFormat)
  }

  protected def deriveRecalcPrimordial(pipeline: Pipeline, daysToPad: Int): String = {
    val primordialSnapDate = getPrimordialSnapshot(pipeline)
    val recalcPrimordialDate = primordialSnapDate.plusDays(daysToPad).toString
    logger.log(Level.INFO, s"PRIMORDIAL DATE: Set for recalculation as $recalcPrimordialDate.")
    recalcPrimordialDate
  }

  /**
   * Deletes all targets states silver and gold. Used to recalculate modules from scratch after snapshotting bronze.
   * Warning: Should be applied to snapshotted pipeline_report table only!
   * TODO: list of modules to delete should be taken from pipeline definition, but is not exposed as a list as of now.
   *
   * @param target
   * @return
   */
  protected def resetPipelineReportState(pipelineStateTable: PipelineTable, primordialDateString: String): Unit = {

    try {
      val sql = s"""delete from ${pipelineStateTable.tableFullName} where moduleID >= 2000"""
      println(s"deleting silver and gold module state entries:\n$sql")
      spark.sql(sql)

      val updatePrimordialDateSql =
        s"""update ${pipelineStateTable.tableFullName}
           |set primordialDateString = '$primordialDateString' where moduleID < 2000""".stripMargin
      println(s"updating primordial date for snapped stated:\n$updatePrimordialDateSql")
      spark.sql(updatePrimordialDateSql)
    } catch {
      case e: Throwable => {
        val errMsg = s"FAILED to updated pipeline state: ${e.getMessage}"
        println(errMsg)
        logger.log(Level.ERROR, errMsg, e)
      }
    }
  }

  /**
   *
   * @param targetDetail
   * @param sourceDB
   * @param incrementalTest
   * @param tol
   * @return
   */
  def assertDataFrameDataEquals(
                                 targetDetail: ModuleTarget,
                                 sourceDB: String,
                                 incrementalTest: Boolean,
                                 tol: Double = 0D,
                                 minimumPaddingDays: Int = 7
                               ): ValidationReport = {
    val module = targetDetail.module
    val sourceTarget = targetDetail.target.copy(_databaseName = sourceDB)
    val snapTarget = targetDetail.target

    val (expected, result) = if (incrementalTest) {
      (
        sourceTarget.asIncrementalDF(module, sourceTarget.incrementalColumns).fillAllNAs,
        snapTarget.asIncrementalDF(module, sourceTarget.incrementalColumns).fillAllNAs
      )
    } else {
      (sourceTarget.asDF.fillAllNAs, snapTarget.asDF.fillAllNAs)
    }

    val expectedCol = "assertDataFrameNoOrderEquals_expected"
    val actualCol = "assertDataFrameNoOrderEquals_actual"

    val keyColNames = sourceTarget.keys :+ "organization_id"
    val validationStatus = s"VALIDATING TABLE: ${snapTarget.tableFullName}\nFROM TIME: ${module.fromTime.asTSString} " +
      s"\nUNTIL TIME: ${module.untilTime.asTSString}\nFOR FIELDS: ${keyColNames.mkString(", ")}"
    logger.log(Level.INFO, validationStatus)

    val validationReport =
      try {

        val expectedElementsCount = expected
          .groupBy(keyColNames map col: _*)
          .agg(count(lit(1)).as(expectedCol))
        val resultElementsCount = result
          .groupBy(keyColNames map col: _*)
          .agg(count(lit(1)).as(actualCol))

        val diff = expectedElementsCount
          .join(resultElementsCount, keyColNames.toSeq, "full_outer")

        // Coalesce used because comparing null and long results in null. when one side is null return diff
        val expectedCountCol = sum(coalesce(col(expectedCol), lit(0L))).alias("tableSourceCount")
        val resultCountCol = sum(coalesce(col(actualCol), lit(0L))).alias("tableSnapCount")
        val discrepancyCol = (expectedCountCol - resultCountCol).alias("totalDiscrepancies")
        val discrepancyPctCol = discrepancyCol / expectedCountCol
        val passFailMessage = when(discrepancyPctCol <= tol, lit("PASS"))
          .otherwise(concat_ws(" ",
            lit("FAIL: Discrepancy of"), discrepancyPctCol, lit("outside of specified tolerance"), lit(tol)
          )).alias("message")

        if (diff.isEmpty) {
          // selecting from empty dataset gives next on empty iterator
          // thus must create the DS manually
          ValidationReport(
            Some(sourceTarget.tableFullName),
            Some(snapTarget.tableFullName),
            Some(expectedElementsCount.count()),
            Some(resultElementsCount.count()),
            Some(0L),
            Some(new Timestamp(module.fromTime.asUnixTimeMilli)),
            Some(new Timestamp(module.untilTime.asUnixTimeMilli)),
            Some("FAIL: No records could be compared")
          )
        } else {
          diff
            .select(
              lit(sourceTarget.tableFullName).alias("tableSourceName"),
              lit(snapTarget.tableFullName).alias("tableSnapName"),
              expectedCountCol,
              resultCountCol,
              discrepancyCol,
              module.fromTime.asColumnTS.alias("from"),
              module.untilTime.asColumnTS.alias("until"),
              passFailMessage
            ).as[ValidationReport].first()
        }
      } catch {
        case e: Throwable => {
          val errMsg = s"FAILED VALIDATION RUN: ${snapTarget.tableFullName} --> ${e.getMessage}"
          logger.log(Level.ERROR, errMsg, e)
          ValidationReport(
            Some(sourceTarget.tableFullName),
            Some(snapTarget.tableFullName), Some(0L), Some(0L), Some(0L),
            Some(new Timestamp(module.fromTime.asUnixTimeMilli)),
            Some(new Timestamp(module.untilTime.asUnixTimeMilli)),
            Some(errMsg)
          )
        }
      }

    validationReport
  }
}
