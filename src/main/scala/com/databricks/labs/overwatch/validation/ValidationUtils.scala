package com.databricks.labs.overwatch.validation

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.concurrent.ForkJoinPool

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.mutable.ParArray


trait ValidationUtils extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  protected def getDateFormat: SimpleDateFormat = getDateFormat(None)

  protected def validateTargetDestruction(targetDBName: String): Unit = {
    val sparkCheckValue = spark.conf.get("overwatch.permit.db.destruction")
    require(sparkCheckValue == targetDBName, s"You selected to run a very destructive command. This is 100% ok in " +
      s"certain circumstances but to protect your data you must set the following spark conf with a value equal " +
      s"to the name of the database to which you're point this function.\n\noverwatch.permit.db.destruction = $targetDBName")
  }

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

  /**
   * Snapshots table
   *
   * @param snapTarget
   * @param params
   * @return
   */
  protected def snapTable(sourceDBName: String, bronzeModule: Module, snapTarget: PipelineTable): SnapReport = {

    val module = bronzeModule.copy(_moduleDependencies = Array[Int]())
    val pipeline = module.pipeline

    try {
      val sourceTarget = snapTarget
        .copy(
          _databaseName = sourceDBName,
          mode = "overwrite",
          withCreateDate = false,
          withOverwatchRunID = false)

      if (!sourceTarget.exists) throw new BronzeSnapException("MISSING SOURCE:", snapTarget, module)
      val finalDFToClone = sourceTarget
        .asIncrementalDF(module, snapTarget.incrementalColumns)
      pipeline.database.write(finalDFToClone, snapTarget, pipeline.pipelineSnapTime.asColumnTS)


      println(s"SNAPPED: ${snapTarget.tableFullName}")
      val snapCount = spark.table(snapTarget.tableFullName).count()

      println(s"UPDATING Module State for ${module.moduleId} --> ${module.moduleId}")
      pipeline.updateModuleState(module.moduleState.copy(
        fromTS = module.fromTime.asUnixTimeMilli,
        untilTS = module.untilTime.asUnixTimeMilli,
        recordsAppended = snapCount
      ))

      snapTarget.asDF()
        .select(
          lit(snapTarget.tableFullName).alias("tableFullName"),
          module.fromTime.asColumnTS.alias("from"),
          module.untilTime.asColumnTS.alias("until"),
          lit(snapCount).alias("totalCount"),
          lit(null).cast("string").alias("errorMessage")
        ).as[SnapReport]
        .first()

    } catch {
      case e: BronzeSnapException =>
        println(e.errMsg)
        logger.log(Level.ERROR, e.errMsg, e)
        e.snapReport
      case e: Throwable =>
        val errMsg = s"FAILED SNAP: ${snapTarget.tableFullName} --> ${e.getMessage}"
        println(errMsg)
        logger.log(Level.ERROR, errMsg, e)
        SnapReport(
          snapTarget.tableFullName,
          new java.sql.Timestamp(module.fromTime.asUnixTimeMilli),
          new java.sql.Timestamp(module.untilTime.asUnixTimeMilli),
          0L,
          errMsg)
    }
  }

  protected def snapStateTables(sourceDBName: String, taskSupport: ForkJoinTaskSupport, bronzePipeline: Pipeline): Unit = {

    val tableLoc = bronzePipeline.BronzeTargets.cloudMachineDetail.tableLocation
    val dropInstanceDetailsSql = s"drop table if exists ${bronzePipeline.BronzeTargets.cloudMachineDetail.tableFullName}"
    println(s"Dropping instanceDetails in SnapDB created from instantiation\n${dropInstanceDetailsSql}")
    spark.sql(dropInstanceDetailsSql)
    dbutils.fs.rm(tableLoc, true)

    val uniqueTablesToClone = Array(
      bronzePipeline.BronzeTargets.processedEventLogs,
      bronzePipeline.BronzeTargets.cloudMachineDetail.copy(mode = "overwrite"),
      bronzePipeline.pipelineStateTarget
    ).par
    uniqueTablesToClone.tasksupport = taskSupport

    uniqueTablesToClone.foreach(target => {
      try {
        val stmt = s"CREATE TABLE ${target.tableFullName} DEEP CLONE ${sourceDBName}.${target.name}"
        println(s"CLONING TABLE ${target.tableFullName}.\nSTATEMENT: ${stmt}")
        spark.sql(stmt)
      } catch {
        case e: Throwable => {
          val errMsg = s"FAILED TO CLONE: ${target.tableFullName}\nERROR: ${e.getMessage}"
          println(errMsg)
          logger.log(Level.ERROR, errMsg, e)
        }
      }
    })
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
   * @return
   */
  protected def resetPipelineReportState(pipelineStateTable: PipelineTable, primordialDateString: String, isRefresh: Boolean = false): Unit = {

    try {
      if (!isRefresh) {
        val sql = s"""delete from ${pipelineStateTable.tableFullName} where moduleID >= 2000"""
        println(s"deleting silver and gold module state entries:\n$sql")
        spark.sql(sql)
      }

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

  protected def validateTargetKeys(targetsToValidate: ParArray[PipelineTable]): Dataset[KeyReport] = {

    targetsToValidate.map(t => {
      val keys = t.keys
      val df = t.asDF()
      val cols = df.columns
      try {
        val baseCount = df.count()
        val keyCount = df.select(keys map col: _*).distinct.count
        val nullKeys = keys.map(k => NullKey(k, df.select(col(k)).filter(col(k).isNull).count()))
        val msg = if (baseCount == keyCount && nullKeys.exists(_.nullCount == 0L)) "PASS" else "FAIL"
        KeyReport(
          t.tableFullName,
          keys,
          baseCount,
          keyCount,
          nullKeys,
          cols,
          msg
        )
      } catch {
        case e: Throwable => {
          val errMsg = s"FAILED: ${t.tableFullName} $e.getMessage"
          logger.log(Level.ERROR, errMsg, e)
          KeyReport(
            t.tableFullName,
            keys,
            0L,
            0L,
            Array[NullKey](),
            cols,
            errMsg
          )
        }
      }
    }).toArray.toSeq.toDS
  }

  protected def dupHunter(targetsToSearch: ParArray[PipelineTable]): (Dataset[DupReport], Array[TargetDupDetail]) = {

    val dupsDetails = targetsToSearch.filterNot(_.name.toLowerCase == "instancedetails").map(t => {
      try {
        val w = Window.partitionBy(t.keys map col: _*).orderBy(t.incrementalColumns map col: _*)
        val selects = t.keys(true) ++ t.incrementalColumns ++ Array("rnk", "rn")

        val nullFilters = t.keys.map(k => col(k).isNotNull)
        val baseDF = nullFilters.foldLeft(t.asDF)((df, f) => df.filter(f))

        val dupsDF = baseDF
          .withColumn("rnk", rank().over(w))
          .withColumn("rn", row_number().over(w))
          .filter('rnk > 1 || 'rn > 1)

        val fullDupsDF = baseDF
          .join(dupsDF.select(t.keys map col: _*), t.keys.toSeq)

        if (dupsDF.isEmpty) {
          val dupReport = DupReport(
            t.tableFullName,
            t.keys,
            t.incrementalColumns,
            0L, 0L, 0L, 0D, 0D,
            "PASS"
          )
          (TargetDupDetail(t, None), dupReport)
        } else {
          val totalDistinctKeys = t.asDF.select(t.keys map col: _*).distinct().count()
          val totalRecords = t.asDF.select(t.keys map col: _*).count()
          val dupReport = dupsDF
            .select(selects.distinct map col: _*)
            .groupBy(t.keys map col: _*)
            .agg(
              countDistinct(col(t.keys.head), (t.keys.tail.map(col) :+ col("rnk") :+ col("rn")): _*)
                .alias("dupsCountByKey")
            )
            .select(
              lit(t.tableFullName).alias("tableName"),
              lit(t.keys).alias("keys"),
              lit(t.incrementalColumns).alias("incrementalColumns"),
              sum('dupsCountByKey).alias("dupCount"),
              countDistinct(col(t.keys.head), t.keys.tail.map(col): _*).alias("keysWithDups")
            )
            .withColumn("totalRecords", lit(totalRecords))
            .withColumn("pctKeysWithDups", 'keysWithDups.cast("double") / lit(totalDistinctKeys))
            .withColumn("pctDuplicateRecords", 'dupCount.cast("double") / lit(totalRecords))
            .withColumn("msg", lit("FAIL"))
            .as[DupReport].first()
          (TargetDupDetail(t, Some(fullDupsDF)), dupReport)
        }
      } catch {
        case e: Throwable => {
          val msg = s"PROCESS FAIL: Table ${t.tableFullName}. ERROR: ${e.getMessage}"
          logger.log(Level.ERROR, msg, e)
          val dupReport = DupReport(
            t.tableFullName,
            t.keys,
            t.incrementalColumns,
            0L, 0L, 0L, 0D, 0D,
            msg
          )
          (TargetDupDetail(t, None), dupReport)
        }
      }
    }).toArray

    val finalDupReport = dupsDetails.map(_._2).toSeq.toDS()
    val targetDupDetails = dupsDetails.map(_._1).filter(_.df.nonEmpty)
    (finalDupReport, targetDupDetails)
  }

  protected def getAllPipelineTargets(workspace: Workspace): Array[PipelineTable] = {
    val bronzePipeline = Bronze(workspace)
    val silverPipeline = Silver(workspace)
    val goldPipeline = Gold(workspace)

    val bronzeTargets = bronzePipeline.getAllTargets
    val silverTargets = silverPipeline.getAllTargets
    val goldTargets = goldPipeline.getAllTargets
    (bronzeTargets ++ silverTargets ++ goldTargets).par.filter(_.exists).toArray
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
