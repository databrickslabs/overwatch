package com.databricks.labs.overwatch.validation

import com.databricks.labs.overwatch.env.Workspace
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline._
import com.databricks.labs.overwatch.utils._
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.mutable.ParArray
import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}


class ValidationUtils(sourceDBName: String, snapWorkspace: Workspace, _paralellism: Option[Int]) extends SparkSessionWrapper {
  //  private var taskSupport =
  private val logger: Logger = Logger.getLogger(this.getClass)
  protected val parallelism: Int = _paralellism.getOrElse(getDriverCores - 1)
  protected val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))


  import spark.implicits._

  /**
   * The following pipeline instantiations utilize an Overwatch private constructor to instantiate the pipelines
   * with specific attributes that enable more simplistic stateful functionality when working with Kitana
   */

  /**
   * Instantiate bronze pipeline
   * @param workspace
   * @param readOnly
   * @param suppressReport
   * @param suppressStaticDatasets
   * @return
   */
  def getBronzePipeline(
                         workspace: Workspace = snapWorkspace,
                         readOnly: Boolean = true,
                         suppressReport: Boolean = true,
                         suppressStaticDatasets: Boolean = true
                       ): Bronze = Bronze(workspace, readOnly, suppressReport, suppressStaticDatasets)

  /**
   * instantiate silver pipeline
   * @param workspace
   * @param readOnly
   * @param suppressReport
   * @param suppressStaticDatasets
   * @return
   */
  def getSilverPipeline(
                         workspace: Workspace = snapWorkspace,
                         readOnly: Boolean = true,
                         suppressReport: Boolean = true,
                         suppressStaticDatasets: Boolean = true
                       ): Silver = Silver(workspace, readOnly, suppressReport, suppressStaticDatasets)

  /**
   * instantiate gold pipeline
   * @param workspace
   * @param readOnly
   * @param suppressReport
   * @param suppressStaticDatasets
   * @return
   */
  def getGoldPipeline(
                       workspace: Workspace = snapWorkspace,
                       readOnly: Boolean = true,
                       suppressReport: Boolean = true,
                       suppressStaticDatasets: Boolean = true
                     ): Gold = Gold(workspace, readOnly, suppressReport, suppressStaticDatasets)

  protected def getDateFormat: SimpleDateFormat = getDateFormat(None)

  /**
   * Disallow the usage of very destructive functions without the explicit, one-time spark config enabled and
   * properly set. This is to protect users from accidentally blowing away their production databases.
   * @param targetDBName
   * @throws com.databricks.labs.overwatch.utils.BadConfigException
   */
  @throws(classOf[BadConfigException])
  protected def validateTargetDestruction(targetDBName: String): Unit = {
    val sparkCheckValue = spark.conf.getOption("overwatch.permit.db.destruction").getOrElse("__OverwatchDBNOTSET")
    require(sparkCheckValue == targetDBName, s"DESTRUCTIVE MODE DISABLED: " +
      s"You selected to run a very destructive command. This is 100% ok in " +
      s"certain circumstances but to protect your data you must set the following spark conf with a value equal " +
      s"to the name of the database to which you're point this function. 'overwatch.permit.db.destruction=$targetDBName''")
  }

  /**
   * simple getter for global default (or overridden) date format
   * @param dtFormatString
   * @return
   */
  protected def getDateFormat(dtFormatString: Option[String]): SimpleDateFormat = {
    val dtFormatStringFinal = dtFormatString.getOrElse(TimeTypesConstants.dtStringFormat)
    new SimpleDateFormat(dtFormatStringFinal)
  }

  /**
   * Requires a minimum number of days (default 3) for a valid kitana test. Recommended duration is 30 days.
   * When attempting to validate an Overwatch dataset with less than 3 days to compare, the accuracy will likely be
   * very misleading
   * @param actualDuration Number of days for Kitana to validate across given the state and config
   * @param recommendedDuration default 30
   * @param minimumDuration default 3
   * @throws com.databricks.labs.overwatch.utils.BadConfigException
   */
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

  /**
   * Requires that the user utilize Kitana as intended to avoid dangerous scenarios to production data.
   * The Kitana db must be created, owned, and maintained by Overwatch
   * @param pipeline
   * @throws com.databricks.labs.overwatch.utils.BadConfigException
   */
  @throws(classOf[BadConfigException])
  protected def validateSnapDatabase(pipeline: Pipeline): Unit = {
    val config = pipeline.config
    val dbProperties = spark.sessionState.catalog.getDatabaseMetadata(config.databaseName).properties
    val isVerifiedSnap = dbProperties.getOrElse("SNAPDB", "FALSE") == "TRUE"
    require(isVerifiedSnap, s"The database, ${config.databaseName} was not created by the Overwatch snapshot " +
      s"process. Any target snap database must be created, owned, and maintained through Overwatch validation " +
      s"processes.")

    if (!config.isLocalTesting) { // temp workaround for dbconnect until ES-99139 is resolved
      if (pipeline.BronzeTargets.auditLogsTarget.exists) { // snap db already exists
        throw new BadConfigException(s"A snapshot may only be created once and it must be created by " +
          s"the overwatch snapshot process. If you would like to create another snapshot and overwrite this one " +
          s"please use Kitana's 'fullResetSnapDB' function in the validation package.")
      }
    }
  }

  /**
   * Returns ModuleTarget with each module by target for the configured scopes
   * A ModuleTarget is essentiall a module combined with it's pipeline Target in a case class. A more formal
   * bond between module and target will be integrated at a later date
   * @param pipeline
   * @return
   */
  protected[overwatch] def getLinkedModuleTarget(pipeline: Pipeline): Seq[ModuleTarget] = {
    pipeline match {
      case rawPipeline: Bronze => {
        val bronzePipeline = rawPipeline.asInstanceOf[Bronze].suppressRangeReport(true)
        val bronzeTargets = bronzePipeline.BronzeTargets
        bronzePipeline.getConfig.overwatchScope.flatMap {
          case OverwatchScope.audit => Seq(ModuleTarget(bronzePipeline.auditLogsModule, bronzeTargets.auditLogsTarget))
          case OverwatchScope.clusters => Seq(ModuleTarget(bronzePipeline.clustersSnapshotModule, bronzeTargets.clustersSnapshotTarget))
          case OverwatchScope.clusterEvents => Seq(ModuleTarget(
            bronzePipeline.clusterEventLogsModule.copy(_hardLimitMaxHistory = None), // Dont set hard max limit for snapshots
            bronzeTargets.clusterEventsTarget)
          )
          case OverwatchScope.jobs => Seq(ModuleTarget(bronzePipeline.jobsSnapshotModule, bronzeTargets.jobsSnapshotTarget))
          case OverwatchScope.pools => Seq(ModuleTarget(bronzePipeline.poolsSnapshotModule, bronzeTargets.poolsSnapshotTarget))
          case OverwatchScope.sparkEvents => Seq(ModuleTarget(bronzePipeline.sparkEventLogsModule, bronzeTargets.sparkEventLogsTarget))
          case _ => Seq[ModuleTarget]()
        }
      }
      case rawPipeline: Silver => {
        val silverPipeline = rawPipeline.asInstanceOf[Silver].suppressRangeReport(true)
        val silverTargets = silverPipeline.SilverTargets
        silverPipeline.config.overwatchScope.flatMap {
          case OverwatchScope.accounts => {
            Seq(
              ModuleTarget(silverPipeline.accountLoginsModule, silverTargets.accountLoginTarget),
              ModuleTarget(silverPipeline.modifiedAccountsModule, silverTargets.accountModTarget)
            )
          }
          case OverwatchScope.notebooks => Seq(ModuleTarget(silverPipeline.notebookSummaryModule, silverTargets.notebookStatusTarget))
          case OverwatchScope.clusters => Seq(ModuleTarget(silverPipeline.clusterSpecModule, silverTargets.clustersSpecTarget))
          case OverwatchScope.sparkEvents => {
            Seq(
              ModuleTarget(silverPipeline.executorsModule, silverTargets.executorsTarget),
              ModuleTarget(silverPipeline.executionsModule, silverTargets.executionsTarget),
              ModuleTarget(silverPipeline.sparkJobsModule, silverTargets.jobsTarget),
              ModuleTarget(silverPipeline.sparkStagesModule, silverTargets.stagesTarget),
              ModuleTarget(silverPipeline.sparkTasksModule, silverTargets.tasksTarget)
            )
          }
          case OverwatchScope.jobs => {
            Seq(
              ModuleTarget(silverPipeline.jobStatusModule, silverTargets.dbJobsStatusTarget),
              ModuleTarget(silverPipeline.jobRunsModule, silverTargets.dbJobRunsTarget)
            )
          }
          case _ => Seq[ModuleTarget]()
        }
      }
      case rawPipeline: Gold => {
        val goldPipeline = rawPipeline.asInstanceOf[Gold].suppressRangeReport(true)
        val goldTargets = goldPipeline.GoldTargets
        goldPipeline.config.overwatchScope.flatMap {
          case OverwatchScope.accounts => {
            Seq(
              ModuleTarget(goldPipeline.accountModModule, goldTargets.accountModsTarget),
              ModuleTarget(goldPipeline.accountLoginModule, goldTargets.accountLoginTarget)
            )
          }
          case OverwatchScope.notebooks => {
            Seq(ModuleTarget(goldPipeline.notebookModule, goldTargets.notebookTarget))
          }
          case OverwatchScope.clusters => {
            Seq(
              ModuleTarget(goldPipeline.clusterModule, goldTargets.clusterTarget),
              ModuleTarget(goldPipeline.clusterStateFactModule, goldTargets.clusterStateFactTarget)
            )
          }
          case OverwatchScope.sparkEvents => {
            Seq(
              ModuleTarget(goldPipeline.sparkExecutorModule, goldTargets.sparkExecutorTarget),
              ModuleTarget(goldPipeline.sparkExecutionModule, goldTargets.sparkExecutionTarget),
              ModuleTarget(goldPipeline.sparkJobModule, goldTargets.sparkJobTarget),
              ModuleTarget(goldPipeline.sparkStageModule, goldTargets.sparkStageTarget),
              ModuleTarget(goldPipeline.sparkTaskModule, goldTargets.sparkTaskTarget)
            )
          }
          case OverwatchScope.jobs => {
            Seq(
              ModuleTarget(goldPipeline.jobsModule, goldTargets.jobTarget),
              ModuleTarget(goldPipeline.jobRunsModule, goldTargets.jobRunTarget),
              ModuleTarget(goldPipeline.jobRunCostPotentialFactModule, goldTargets.jobRunCostPotentialFactTarget)
            )
          }
          case _ => Seq[ModuleTarget]()
        }
      }
    }
  }

  /**
   * Create the clone statement for bronze targets
   * @param target
   * @return
   */
  private def buildCloneStatement(target: PipelineTable): String = {
    val baseStatement = s"CREATE OR REPLACE TABLE ${target.tableFullName} SHALLOW CLONE ${sourceDBName}.${target.name} "
    val locationClause = s"LOCATION '${target.tableLocation}' "
    baseStatement + locationClause
  }

  /**
   * Snapshot a bronze table given its state provided by a linked module derived from the config
   * @param bronzeModule stateful module deried from the config
   * @param snapTarget PipelineTable built out by the corresponding module
   * @param snapUntilTime TimeType of the pipeline snapshot
   * @return
   */
  protected def snapTable(
                           bronzeModule: Module,
                           snapTarget: PipelineTable,
                           snapUntilTime: TimeTypes
                         ): SnapReport = {

    val module = bronzeModule.copy(_moduleDependencies = Array[Int]())

    try {

      val stmt = buildCloneStatement(snapTarget)
      println(s"CLONING TABLE ${snapTarget.tableFullName}.\nSTATEMENT: ${stmt}")
      spark.sql(stmt)

      val snapLogMsg = s"SNAPPED: ${snapTarget.tableFullName}\nFROM: ${module.fromTime.asDTString}\nTO: " +
        s"${module.untilTime.asDTString}"
      println(snapLogMsg)
      logger.log(Level.INFO, snapLogMsg)

      snapTarget.asDF()
        .select(
          lit(snapTarget.tableFullName).alias("tableFullName"),
          module.fromTime.asColumnTS.alias("from"), // pipeline state has been cleared, fromTime == primordial
          snapUntilTime.asColumnTS.alias("until"),
          lit(null).cast("string").alias("errorMessage")
        ).as[SnapReport]
        .first()

    } catch {
      case e: Throwable =>
        val errMsg = s"FAILED SNAP: ${snapTarget.tableFullName} --> ${e.getMessage}"
        println(errMsg)
        logger.log(Level.ERROR, errMsg, e)
        throw new BronzeSnapException(errMsg, snapTarget, module)
    }
  }

  /**
   * There are state management and status tables within the Overwatch Database that provide overall pipeline state.
   * These tables must be handled slightly different to ensure the proper state is loaded when the pipeline is
   * initialized
   * @param bronzePipeline
   * @param untilTS
   * @return
   */
  protected def snapStateTables(
                                 bronzePipeline: Pipeline,
                                 untilTS: TimeTypes
                               ): Seq[SnapReport] = {

    val uniqueTablesToClone = Array(
      bronzePipeline.BronzeTargets.processedEventLogs,
      bronzePipeline.BronzeTargets.cloudMachineDetail,
      bronzePipeline.pipelineStateTarget
    ).par
    uniqueTablesToClone.tasksupport = taskSupport

    uniqueTablesToClone.map(target => {
      try {
        val stmt = buildCloneStatement(target)
        println(s"CLONING TABLE ${target.tableFullName}.\nSTATEMENT: ${stmt}")
        spark.sql(stmt)
        SnapReport(
          target.tableFullName,
          new java.sql.Timestamp(bronzePipeline.primordialTime.asUnixTimeMilli),
          new java.sql.Timestamp(untilTS.asUnixTimeMilli),
          "null"
        )
      } catch {
        case e: Throwable => {
          val errMsg = s"FAILED TO CLONE: $sourceDBName.${target.name}\nERROR: ${e.getMessage}"
          println(errMsg)
          logger.log(Level.ERROR, errMsg, e)
          throw new BronzeSnapException(errMsg, target, Module(0, "STATE_SNAP", bronzePipeline))
        }
      }
    }).toArray.toSeq
  }

  /**
   * Overwatch data quality is much more accurate after given a few days to initialize all the Databricks
   * workspace objects. As such, Kitana needs to validate an interior subset of the existing data; therefore,
   * a padding is applied to the primordial date to provide more startup data so as to increase accuracy of
   * derivations to closer of that of production.
   *
   * Left padding is defined by daysToPad
   * Right padding should not be necessary as the maxDays provided should limit the number of days on the right,
   * if it does not, 2 days are automatically applied as padding to the right side
   * @param pipeline Snapshot pipeline to which the padding will be applied
   * @param daysToPad Number of days to pad from the starting point
   * @param maxDays Total number of days from starting point
   * @return
   */
  protected def padPrimordialAndSetMaxDays(pipeline: Pipeline, daysToPad: Int, maxDays: Option[Int]): Workspace = {
    if (pipeline.getPipelineState.isEmpty) {
      val errorMsg = "The state of the bronze snapshot cannot be determine. A bronze snapshot " +
        "must exist prior to executing Silver/Gold recalculations/validations. " +
        "If you haven't yet run created the snapshot you " +
        "may do so by running Kitana.executeBronzeSnapshot(...)"
      throw new PipelineStateException(errorMsg, None)
    }

    // derive dates and total bronze duration
    val snappedPrimordialString = pipeline.getPipelineState.values
      .minBy(state => Pipeline.deriveLocalDate(state.primordialDateString.get, getDateFormat).toEpochDay)
      .primordialDateString.get
    logger.log(Level.INFO, s"Snapped Primordial String: $snappedPrimordialString")

    val snappedPrimordialTime = Pipeline.createTimeDetail(
      Pipeline.deriveLocalDate(snappedPrimordialString, getDateFormat)
        .atStartOfDay(Pipeline.systemZoneId).toInstant.toEpochMilli
    )
    logger.log(Level.INFO, s"snappedPrimordialTimeDateString: ${snappedPrimordialTime.asTSString}")

    //    val lookupPipelineMaxUntil = Pipeline.createTimeDetail(
    //      pipeline.getPipelineState.values.toArray
    //        .filter(_.moduleID >= 2000)
    //        .maxBy(_.untilTS).untilTS
    //    )
    val lookupPipelineMaxUntil = Pipeline.createTimeDetail(pipeline.getPipelineState.values.toArray.maxBy(_.untilTS).untilTS)
    logger.log(Level.INFO, s"lookupPipelineMaxUntil: ${lookupPipelineMaxUntil.asTSString}")

    val daysInScope = Duration.between(
      snappedPrimordialTime.asLocalDateTime.toLocalDate.atStartOfDay(),
      lookupPipelineMaxUntil.asLocalDateTime.toLocalDate.atStartOfDay()
    ).toDays.toInt - daysToPad
    logger.log(Level.INFO, s"daysInScope: $daysInScope")

    // default right padding when maxDays is empty is 2 days.
    val calculatedMaxDays = maxDays.getOrElse(daysInScope - 2)
    logger.log(Level.INFO, s"calculatedMaxDays: $calculatedMaxDays")

    // set config and returns modified workspace
    val primordialDatePlusPadding = snappedPrimordialTime.asLocalDateTime.plusDays(daysToPad).toLocalDate.toString
    val modifiedConfig = pipeline.config
    modifiedConfig.setPrimordialDateString(Some(primordialDatePlusPadding))
    modifiedConfig.setMaxDays(calculatedMaxDays)
    logger.log(Level.INFO, s"MAX DAYS: Updated to $calculatedMaxDays")
    logger.log(Level.INFO, s"PRIMORDIAL DATE: Set for recalculation as $primordialDatePlusPadding.")
    validateDuration(calculatedMaxDays)
    pipeline.workspace.copy(_config = modifiedConfig)
  }

  /**
   * Helper for Overwatch pipeline state movement through time
   * @param pipelineStateTable
   * @param primordialDateString
   */
  private def updateStateTable(
                                pipelineStateTable: PipelineTable,
                                primordialDateString: String
                              ): Unit = {
    val dropSilverGoldStateSql = s"""delete from ${pipelineStateTable.tableFullName} where moduleID >= 2000"""
    val dropSqlMsg = s"deleting silver and gold module state entries\nSTATEMENT: $dropSilverGoldStateSql"
    println(dropSqlMsg)
    logger.log(Level.INFO, dropSqlMsg)
    spark.sql(dropSilverGoldStateSql)

    val updatePrimordialDateSql =
      s"""update ${pipelineStateTable.tableFullName}
         |set primordialDateString = '$primordialDateString' where moduleID < 2000""".stripMargin
    println(s"updating primordial date for snapped stated:\n$updatePrimordialDateSql")
    spark.sql(updatePrimordialDateSql)
  }

  /**
   * DANGEROUS but very efficient method for dropping an array of targets
   * @param targetsToDrop
   */
  protected def fastDropTargets(targetsToDrop: ParSeq[PipelineTable]): Unit = {
    targetsToDrop.tasksupport = taskSupport
    targetsToDrop.map(_.databaseName).foreach(validateTargetDestruction)
    targetsToDrop.foreach(t => {
      Helpers.fastDrop(t, snapWorkspace.getConfig.cloudProvider)
    })
    spark.conf.unset("overwatch.permit.db.destruction")
  }

  /**
   * Deletes all targets states silver and gold. Used to recalculate modules from scratch after snapshotting bronze.
   * Warning: Should be applied to snapshotted pipeline_report table only!
   * TODO: list of modules to delete should be taken from pipeline definition, but is not exposed as a list as of now.
   *
   * @return
   */
  protected def resetPipelineReportState(
                                          bronzePipeline: Bronze,
                                          moduleTargets: Array[ModuleTarget],
                                          fromTime: TimeTypes,
                                          untilTime: TimeTypes
                                        ): Unit = {
    try {
      val pipelineStateTable = bronzePipeline.pipelineStateTarget
      val modulesToBeReset = moduleTargets.map(_.module.moduleId)
      val w = Window.partitionBy('moduleID).orderBy('Pipeline_SnapTS.desc)

      val latestMatchedModules = pipelineStateTable.asDF
        .filter('status === "SUCCESS" || 'status.startsWith("EMPTY"))
        .filter('moduleId.isin(modulesToBeReset: _*)) // limit to the modules identified from the scope
        .withColumn("rnk", rank().over(w))
        .filter('rnk === 1)
        .drop("rnk")
        .withColumn("primordialDateString", lit(fromTime.asDTString))

      updateStateTable(pipelineStateTable, fromTime.asDTString)

      // NOTE: NOT SUPPORTED via DBConnect
      DeltaTable.forName(pipelineStateTable.tableFullName)
        .as("target")
        .merge(
          latestMatchedModules.as("src"),
          "src.Overwatch_RunID = target.Overwatch_RunID AND src.moduleID = target.moduleID"
        )
        .whenMatched
        .updateExpr(Map(
          "fromTS" -> fromTime.asUnixTimeMilli.toString,
          "untilTS" -> untilTime.asUnixTimeMilli.toString,
          "primordialDateString" -> s"'${fromTime.asDTString}'"
        ))
        .execute()
    }

    catch {
      case e: BadConfigException if e.getMessage.contains("DESTRUCTIVE MODE DISABLED") => {
        println(e.getMessage)
        throw new BadConfigException(e.getMessage)
      }
      case e: Throwable => {
        val errMsg = s"FAILED to update pipeline state: ${e.getMessage}"
        println(errMsg)
        logger.log(Level.ERROR, errMsg, e)
      }
    }
  }

  /**
   * Method used to validate proper keys associated with a pipeline table. Tests include
   * 1. uniqueness of keys
   * 2. nonNull keys
   * @param targetsToValidate
   * @return
   */
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
          val errMsg = s"FAILED: ${
            t.tableFullName
          } $e.getMessage"
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

  /**
   * Helper method to identify duplicates within a pipeline table by its keys
   * @param targetsToSearch
   * @return
   */
  protected def dupHunter(targetsToSearch: ParArray[PipelineTable]): (Dataset[DupReport], Array[TargetDupDetail]) = {

    val dupsDetails = targetsToSearch.filterNot(_.name.toLowerCase == "instancedetails").map(t => {
      try {
        val w = Window.partitionBy(t.keys map col: _*).orderBy(t.incrementalColumns map col: _*)
        val selects = t.keys(true) ++ t.incrementalColumns ++ Array("rnk", "rn")
        val totalRecords = t.asDF.select(t.keys map col: _*).count()
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
            0L, 0L, totalRecords, 0D, 0D,
            "PASS"
          )
          (TargetDupDetail(t, None), dupReport)
        } else {
          val totalDistinctKeys = t.asDF.select(t.keys map col: _*).distinct().count()
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
          val msg = s"PROCESS FAIL: Table ${
            t.tableFullName
          }. ERROR: ${
            e.getMessage
          }"
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

  /**
   * quick way to get all pipeline tables as "targets"
   * @param workspace
   * @param includePipelineStateTable
   * @return
   */
  protected def getAllKitanaTargets(workspace: Workspace, includePipelineStateTable: Boolean = false): Array[PipelineTable] = {

    val bronzePipeline = getBronzePipeline(workspace)

    val bronzeTargets = bronzePipeline.getAllTargets
    val silverTargets = getSilverPipeline(workspace).getAllTargets
    val goldTargets = getGoldPipeline(workspace).getAllTargets
    val targets = if (includePipelineStateTable) bronzeTargets ++ silverTargets ++ goldTargets :+ bronzePipeline.pipelineStateTarget
    else bronzeTargets ++ silverTargets ++ goldTargets

    targets.par.filter(_.exists).toArray
  }

  /**
   * Primary validation function for Kitana comparison
   * @param targetDetail
   * @param sourceDB
   * @param tol tolerance of non-matching records as a percentage between 0.0 and 1.0
   * @return
   */
  protected def assertDataFrameDataEquals(
                                           targetDetail: ModuleTarget,
                                           sourceDB: String,
                                           tol: Double
                                         ): ValidationReport = {
    val module = targetDetail.module
    val sourceTarget = targetDetail.target.copy(_databaseName = sourceDB)
    val snapTarget = targetDetail.target

    val (expected, result) = (
      sourceTarget.asIncrementalDF(module, sourceTarget.incrementalColumns).fillAllNAs,
      snapTarget.asIncrementalDF(module, sourceTarget.incrementalColumns).fillAllNAs
    )

    val expectedIsEmpty = expected.isEmpty
    val resultIsEmpty = result.isEmpty

    val expectedCol = "assertDataFrameNoOrderEquals_expected"
    val actualCol = "assertDataFrameNoOrderEquals_actual"

    val keyColNames = sourceTarget.keys :+ "organization_id"
    val validationStatus = s"VALIDATING TABLE: ${
      snapTarget.tableFullName
    }\nFROM TIME: ${
      module.fromTime.asTSString
    } " +
      s"\nUNTIL TIME: ${
        module.untilTime.asTSString
      }\nFOR FIELDS: ${
        keyColNames.mkString(", ")
      }"
    logger.log(Level.INFO, validationStatus)

    val validationReport = try {
      if (expectedIsEmpty || resultIsEmpty) {
        val msg = if (expectedIsEmpty) s"Source: ${
          sourceTarget.tableFullName
        } did not return any data "
        else s"Validation Target: ${
          snapTarget.tableFullName
        } did not return any data "
        val betweenMsg = s"for incrementals between ${
          module.fromTime.asTSString
        } AND ${
          module.untilTime.asTSString
        }"
        throw new NoNewDataException(msg + betweenMsg, Level.ERROR)
      }

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
      val discrepancyCol = abs(expectedCountCol - resultCountCol).alias("totalDiscrepancies")
      val discrepancyPctCol = discrepancyCol / expectedCountCol
      val passFailMessage = when(discrepancyPctCol <= tol, concat_ws(" ", lit("PASS:"),
        discrepancyPctCol, lit("<="), lit(tol)
      ))
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
      case e: NoNewDataException =>
        logger.log(Level.ERROR, e.getMessage, e)
        ValidationReport(
          Some(sourceTarget.tableFullName),
          Some(snapTarget.tableFullName), Some(0L), Some(0L), Some(0L),
          Some(new Timestamp(module.fromTime.asUnixTimeMilli)),
          Some(new Timestamp(module.untilTime.asUnixTimeMilli)),
          Some(e.getMessage)
        )
      case e: Throwable =>
        val errMsg = s"FAILED VALIDATION RUN: ${
          snapTarget.tableFullName
        } --> ${
          e.getMessage
        }"
        logger.log(Level.ERROR, errMsg, e)
        ValidationReport(
          Some(sourceTarget.tableFullName),
          Some(snapTarget.tableFullName), Some(0L), Some(0L), Some(0L),
          Some(new Timestamp(module.fromTime.asUnixTimeMilli)),
          Some(new Timestamp(module.untilTime.asUnixTimeMilli)),
          Some(errMsg)
        )
    }

    validationReport
  }
}
