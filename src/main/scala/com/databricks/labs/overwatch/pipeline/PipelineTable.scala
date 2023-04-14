package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.MergeScope.MergeScope
import com.databricks.labs.overwatch.utils.WriteMode.WriteMode
import com.databricks.labs.overwatch.utils._
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}

//noinspection ScalaCustomHdfsFormat
// TODO -- Add rules: Array[Rule] to enable Rules engine calculations in the append
//  also add ruleStrategy: Enum(Kill, Quarantine, Ignore) to determine when to require them
//  Perhaps add the strategy into the Rule definition in the Rules Engine
case class PipelineTable(
                          name: String,
                          private val _keys: Array[String],
                          config: Config,
                          incrementalColumns: Array[String] = Array(),
                          format: String = "delta", // TODO -- Convert to Enum
                          persistBeforeWrite: Boolean = false,
                          _mode: WriteMode = WriteMode.append,
                          mergeScope: MergeScope = MergeScope.full,
                          maxMergeScanDates: Int = 33, // used to create explicit date merge condition -- should be removed after merge dynamic partition pruning is enabled DBR 11.x LTS
                          private val _permitDuplicateKeys: Boolean = true,
                          private val _databaseName: String = "default",
                          autoOptimize: Boolean = false,
                          autoCompact: Boolean = false,
                          partitionBy: Seq[String] = Seq(),
                          statsColumns: Array[String] = Array(),
                          optimizeFrequency_H: Int = 24 * 7,
                          zOrderBy: Array[String] = Array(),
                          vacuum_H: Int = 24 * 7, // TODO -- allow config overrides -- no vacuum == 0
                          enableSchemaMerge: Boolean = true,
                          withCreateDate: Boolean = true,
                          withOverwatchRunID: Boolean = true,
                          workspaceName: Boolean = true,
                          isTemp: Boolean = false,
                          checkpointPath: Option[String] = None,
                          masterSchema: Option[StructType] = None
                        ) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  val databaseName: String = if (_databaseName == "default") config.databaseName else _databaseName
//  val tableFullName: String = s"${databaseName}.${name}"

  val tableFullName: String = if(config.deploymentType!="default")
    s"${config.etlCatalogName}.${databaseName}.${name}" else s"${databaseName}.${name}"


  // Minimum Schema Enforcement Management
  private var withMasterMinimumSchema: Boolean = if (masterSchema.nonEmpty) true else false
  private var enforceNonNullable: Boolean = if (masterSchema.nonEmpty) true else false
  private var _existsInCatalogConfirmed: Boolean = false
  private var _existsInPathConfirmed: Boolean = false
  private var _existsInDataConfirmed: Boolean = false

  def isStreaming: Boolean = if(checkpointPath.nonEmpty) true else false

  private def emitMissingMasterSchemaMessage(): Unit = {
    val msg = s"No Master Schema defined for Table $tableFullName"
    logger.log(Level.ERROR, msg)
    if (config.debugFlag) println(msg)
  }

  private[overwatch] def withMinimumSchemaEnforcement: this.type = withMinimumSchemaEnforcement(true)

  private[overwatch] def withMinimumSchemaEnforcement(value: Boolean): this.type = {
    if (masterSchema.nonEmpty) withMasterMinimumSchema = value
    else emitMissingMasterSchemaMessage() // cannot enforce master schema if not defined
    this
  }

  private[overwatch] def enforceNullableRequirements: this.type = enforceNullableRequirements(true)

  private[overwatch] def enforceNullableRequirements(value: Boolean): this.type = {
    if (masterSchema.nonEmpty && exists) enforceNonNullable = value
    else emitMissingMasterSchemaMessage() // cannot enforce master schema if not defined
    this
  }

  def writeMode: WriteMode = { // initialize to constructor value
    if (!exists(dataValidation = true) && _mode == WriteMode.merge) {
      val onetimeModeChangeMsg = s"MODE CHANGED from MERGE to APPEND. Target ${tableFullName} does not exist, first write will be " +
        s"performed as an append, subsequent writes will be written as merge to this target"
      if (config.debugFlag) println(onetimeModeChangeMsg)
      logger.log(Level.INFO, onetimeModeChangeMsg)
      WriteMode.append
    } else _mode
  }

  def permitDuplicateKeys: Boolean = if (writeMode == WriteMode.merge) false else _permitDuplicateKeys

  private def applyAutoOptimizeConfigs(): Unit = {

    var overrides = Map[String, String]()

    if (autoOptimize) {
      logger.log(Level.INFO, s"enabling optimizeWrite on $tableFullName")
      overrides = overrides ++ Map("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite" -> "true")
      if (config.debugFlag) println(s"Setting Auto Optimize for ${name}")
    } else overrides = overrides ++ Map("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite" -> "false")

    if (autoCompact) {
      logger.log(Level.INFO, s"enabling autoCompact on $tableFullName")
      overrides = overrides ++ Map("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact" -> "true")
      if (config.debugFlag) println(s"Setting Auto Compact for ${name}")
    } else overrides = overrides ++ Map("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact" -> "false")

    PipelineFunctions.setSparkOverrides(spark, overrides, config.debugFlag)
  }

  def tableIdentifier: Option[TableIdentifier] = {
    val matchingTables = spark.sessionState.catalog.listTables(databaseName, name)
    if (matchingTables.length > 1) {
      throw new Exception(s"MULTIPLE TABLES MATCHED: $tableFullName")
    } else matchingTables.headOption
  }

  def catalogTable: CatalogTable = {
    if (tableIdentifier.nonEmpty) {
      spark.sessionState.catalog.getTableMetadata(tableIdentifier.get)
    } else {
      throw new Exception(s"TARGET TABLE NOT FOUND: $tableFullName")
    }
  }

  //  val isManaged = tblMeta.tableType.name == "MANAGED"
  //  val tblStoragePath = tblMeta.location.toString
  //  DeltaTable.forName(tableFullName).
  val tableLocation: String = s"${config.etlDataPathPrefix}/$name".toLowerCase

  /**
   * is the schema evolving
   * @return
   */
  def isEvolvingSchema: Boolean = {
    name match { // when exists -- locking not necessary if schema does not evolve
      case "pipeline_report" => false
      case "instanceDetails" => false
      case "dbuCostDetails" => false
      case "spark_events_processedFiles" => false
      case _ => true
    }
  }

  private[overwatch] def requiresLocking: Boolean = {
    // If target's schema is evolving or does not exist true
    val enableLocking = if (!exists || isEvolvingSchema) true else false
    val logMsg = if(enableLocking) {
      s"LOCKING ENABLED for table $name"
    } else {
      s"LOCKING DISABLED for table $name"
    }
    logger.log(Level.INFO, logMsg)
    enableLocking
  }

  /**
   * Does the target exist in the catalog
   * If not previously validated, perform test and return result
   *
   * @param test Should this test be executed, if not return true
   * @return
   */
  private def existsInCatalog(test: Boolean): Boolean = {
    if (test && !_existsInCatalogConfirmed) {
      _existsInCatalogConfirmed = spark.catalog.tableExists(tableFullName)
      _existsInCatalogConfirmed
    } else true
  }

  /**
   * Does the target exist in the path
   * Path validation for delta format targets is path/_delta_log others is just path
   * If not previously validated, perform test and return result
   *
   * @param test Should this test be executed, if not return true
   * @return
   */
  private def existsInPath(test: Boolean): Boolean = {
    if (test && !_existsInPathConfirmed) {
      if (format == "delta") { // if delta verify the _delta_log is present not just the path
        _existsInPathConfirmed = Helpers.pathExists(s"$tableLocation/_delta_log")
      } else { // not delta verify the parent dir exists
        _existsInPathConfirmed = Helpers.pathExists(tableLocation)
      }
      _existsInPathConfirmed
    } else true
  }

  /**
   * Does the target exist in the data
   * If not previously validated, perform test and return result
   * @param test Should this test be executed, if not return true
   * @return
   */
  private def existsInData(test: Boolean): Boolean = {
    if (test && !_existsInDataConfirmed) {
      try {
        _existsInDataConfirmed = !spark.read.format("delta")
          .load(tableLocation)
          .filter(col("organization_id") === config.organizationId)
          .isEmpty
      } catch {
        case _: Throwable => _existsInDataConfirmed = false
      }
      _existsInDataConfirmed
    } else true
  }

  /**
   * default catalog only validation
   *
   * @return
   */
  def exists: Boolean = {
    exists()
  }

  /**
   * Does a table exists as defined by
   * Once a target has been validated via a certain method it will not be validated again for this target instance
   * This is done for performance reasons
   *
   * @param pathValidation    does the path exist for the source -- even if the catalog table does not
   * @param dataValidation    is data present for this organizationId (workspaceId) in the source
   * @param catalogValidation does the catalog table exist for this source -- even if the path does not or is empty
   * @return
   */
  def exists(pathValidation: Boolean = true, dataValidation: Boolean = false, catalogValidation: Boolean = false): Boolean = {
    // If target already confirmed to exist via a specific method just return that it exists
    //  only determine once per instance of PipelineTable
        existsInCatalog(catalogValidation) &&
          existsInPath(pathValidation) &&
          existsInData(dataValidation)
  }

  /**
   * returns all keys including partition columns
   * @return
   */
  def keys: Array[String] = keys()

  /**
   * returns all keys including partition columns EXCEPT the overwatch_ctrl_noise which is used for localized
   * noise to reduce skew, not a business key
   * @param withOverwatchMeta if true, returns Overwatch_RunID and Pipeline_SnapTS as part of the keys
   * @return
   */
  def keys(withOverwatchMeta: Boolean = false): Array[String] = {
    //
    val baselineKeys = (_keys ++ partitionBy :+ "organization_id").filterNot(_ == "__overwatch_ctrl_noise").distinct
    if (withOverwatchMeta) {
      (baselineKeys ++ Array("Overwatch_RunID", "Pipeline_SnapTS")).distinct
    } else {
      baselineKeys.distinct
    }
  }

  def asDF: DataFrame = {
    asDF()
  }

  def asDF(withGlobalFilters: Boolean = true): DataFrame = {
    val noExistsMsg = s"${tableFullName} does not exist or cannot apply global filters"
    try {
      if (exists) {
        val fullDF = if (withMasterMinimumSchema) { // infer master schema if true and available
          logger.log(Level.INFO, s"SCHEMA -> Minimum Schema enforced for $tableFullName")
          spark.read.format(format).load(tableLocation).verifyMinimumSchema(masterSchema, enforceNonNullable, config.debugFlag)
        } else spark.read.format(format).load(tableLocation)
        if (withGlobalFilters && config.globalFilters.nonEmpty)
          PipelineFunctions.applyFilters(fullDF, config.globalFilters, config.debugFlag)
        else fullDF
      } else {
        logger.log(Level.WARN, noExistsMsg)
        if (config.debugFlag) println(noExistsMsg)
        spark.emptyDataFrame
      }
    } catch {
      case e: AnalysisException =>
        logger.log(Level.WARN, noExistsMsg, e)
        Array(s"Could not retrieve ${tableFullName}").toSeq.toDF("ERROR")
    }
  }

  @throws(classOf[UnhandledException])
  def asIncrementalDF(module: Module, cronColumns: String*): DataFrame = {
    asIncrementalDF(module, cronColumns, 0)
  }

  @throws(classOf[UnhandledException])
  def asIncrementalDF(module: Module, additionalLagDays: Int, cronColumns: String*): DataFrame = {
    asIncrementalDF(module, cronColumns, additionalLagDays)
  }

  private def casedCompare(s1: String, s2: String): Boolean = {
    if ( // make lower case if column names are case insensitive
      spark.conf.getOption("spark.sql.caseSensitive").getOrElse("false").toBoolean
    ) s1 == s2 else s1.equalsIgnoreCase(s2)
  }

  /**
   * Build 1 or more incremental filters for a dataframe from standard start or aditionalStart less
   * "additionalLagDays" when loading an incremental DF with some front-padding to capture lagging start events
   * IMPORTANT: This logic is insufficient for date only filters. Date filters are meant to also be partition filters
   * coupled with a more granular filter. Date filters are inclusive on both sides for paritioning.
   *
   * @param module            module used for logging and capturing status timestamps by module
   * @param additionalLagDays Front-padded days prior to start
   * @param cronColumnsNames  1 or many incremental columns
   * @return Filtered Dataframe
   */
  def asIncrementalDF(
                       module: Module,
                       cronColumnsNames: Seq[String],
                       additionalLagDays: Long = 0
                     ): DataFrame = {
    val moduleId = module.moduleId
    val moduleName = module.moduleName
    val noExistsMsg = s"${tableFullName} does not exist or cannot apply global filters"

    if (exists) {
      val instanceDF = if (withMasterMinimumSchema) { // infer master schema if true and available
        logger.log(Level.INFO, s"SCHEMA -> Minimum Schema enforced for Module: " +
          s"$moduleId --> $moduleName for Table: $tableFullName")
        spark.read.format(format).load(tableLocation).verifyMinimumSchema(masterSchema, enforceNonNullable, config.debugFlag)
      } else spark.read.format(format).load(tableLocation)
      val incrementalFilters = PipelineFunctions.buildIncrementalFilters(
        this, instanceDF, module.fromTime, module.untilTime, additionalLagDays, moduleName
      )

      PipelineFunctions.withIncrementalFilters(
        instanceDF, Some(module), incrementalFilters, config.globalFilters, config.debugFlag
      )
    } else { // Source doesn't exist
      logger.log(Level.WARN, noExistsMsg)
      if (config.debugFlag) println(noExistsMsg)
      spark.emptyDataFrame
    }
  }

  def writer(df: DataFrame): Any = { // TODO -- don't return any, return instance of trait
    applyAutoOptimizeConfigs()
    if (writeMode != WriteMode.merge) { // DELTA Writer Built at write time
      if (checkpointPath.nonEmpty) { // IS STREAMING
        val streamWriterMessage = s"DEBUG: PipelineTable - Checkpoint for ${tableFullName} == ${checkpointPath.get}"
        if (config.debugFlag) println(streamWriterMessage)
        logger.log(Level.INFO, streamWriterMessage)
        var streamWriter = df.writeStream.outputMode(writeMode.toString).format(format).option("checkpointLocation", checkpointPath.get)
          .queryName(s"StreamTo_${name}_${config.organizationId}")
        streamWriter = if (partitionBy.nonEmpty) streamWriter.partitionBy(partitionBy: _*) else streamWriter // add partitions if exists
        streamWriter = if (writeMode == WriteMode.overwrite) { // set overwrite && set overwriteSchema == true
          streamWriter.option("overwriteSchema", "true")
        } else if (enableSchemaMerge && writeMode != WriteMode.overwrite) { // append AND merge schema
          streamWriter
            .option("mergeSchema", "true")
        } else streamWriter // append AND overwrite schema
        streamWriter // Return built Stream Writer
          .option("userMetadata", config.runID)
      } else { // NOT STREAMING
        var dfWriter = df.write.mode(writeMode.toString).format(format)
        dfWriter = if (partitionBy.nonEmpty) dfWriter.partitionBy(partitionBy: _*) else dfWriter // add partitions if exists
        dfWriter = if (writeMode == WriteMode.overwrite) { // set overwrite && set overwriteSchema == true
          dfWriter.option("overwriteSchema", "true")
        } else if (enableSchemaMerge && writeMode != WriteMode.overwrite) { // append AND merge schema
          dfWriter
            .option("mergeSchema", "true")
        } else dfWriter // append AND overwrite schema
        dfWriter // Return built Batch Writer
          .option("userMetadata", config.runID)
      }
    }
  }

  /**
   * return duplicate records globally
   * @return
   */
  def getDups(): DataFrame = {
    val df = if (this.exists) this.asDF(withGlobalFilters = false) else spark.emptyDataFrame
    val w = Window.partitionBy(keys map col: _*).orderBy(incrementalColumns map col: _*)
    val dups = df
      .withColumn("rnk", rank().over(w))
      .withColumn("rn", row_number().over(w))
      .filter('rnk > 1 || 'rn > 1)
      .drop("rn", "rnk")
      .select(keys map col: _*)
    df
      .join(dups, keys)
      .orderBy(incrementalColumns map col: _*)
  }

  /**
   * Deletes duplicates for all workspaces configured in the table
   * @param tableFilters filter to exclude certain table names
   */
  def deleteDups(tableFilters: Array[Column] = Array()): Unit = {
    if (this.exists) {
      val completeKeySet = (keys ++ incrementalColumns).map(_.toLowerCase).distinct
      val w = Window.partitionBy(completeKeySet map col: _*).orderBy(incrementalColumns map col: _*)
      val wRank = Window.partitionBy((completeKeySet :+ "rnk") map col: _*).orderBy(incrementalColumns map col: _*)
      val startVersion = spark.sql(s"desc history $tableFullName").select(max('version)).as[Long].first
      val db = tableFullName.split("\\.")(0)
      val tbl = tableFullName.split("\\.")(1)
      val tblLocation = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl, Some(db))).location.toString
      val conditionalMatchClause = completeKeySet.map(c => col(s"source.$c") === col(s"target.$c")).reduce((x, y) => x && y)

      val nonNullFilters = completeKeySet.map(k => col(k).isNotNull) ++ tableFilters
      val currentDF = nonNullFilters.foldLeft(spark.table(tableFullName))((df, f) => df.filter(f))
      val vnDF = nonNullFilters.foldLeft(spark.read.format("delta").option("versionAsOf", startVersion).load(tblLocation))((df, f) => df.filter(f))

      val dupsToDelete = currentDF
        .withColumn("rnk", rank().over(w))
        .withColumn("rn", row_number().over(w))
        .filter('rnk > 1 || 'rn > 1)

      val dupsToRestore = vnDF
        .withColumn("rnk", rank().over(w))
        .withColumn("rn", row_number().over(wRank))
        .filter('rnk === 1 && 'rn === 2)
        .drop("rnk", "rn")

      DeltaTable.forName(spark, tableFullName)
        .as("target")
        .merge(
          dupsToDelete.as("source"), conditionalMatchClause
        )
        .whenMatched
        .delete()
        .execute

      DeltaTable.forName(spark, tableFullName)
        .as("target")
        .merge(
          dupsToRestore.as("source"), conditionalMatchClause
        )
        .whenNotMatched()
        .insertAll()
        .execute

      val wHist = Window.orderBy('version.desc)
      val hist = spark.sql(s"desc history $tableFullName")
        .filter('operation === "MERGE")
        .withColumn("rnk", rank().over(wHist))
        .withColumn("rn", row_number().over(wHist))

      val dupsFound = hist.filter('rnk === 2).select($"operationMetrics.numTargetRowsDeleted".cast("long")).as[Long].first
      val originalsRecovered = hist.filter('rnk === 1).select($"operationMetrics.numTargetRowsInserted".cast("long")).as[Long].first
      val dupsDropped = dupsFound - originalsRecovered
      println(s"DROPPED $dupsDropped duplicate records from $tableFullName")
    } else println(s"$tableFullName does not exist: SKIPPING")
  }


}
