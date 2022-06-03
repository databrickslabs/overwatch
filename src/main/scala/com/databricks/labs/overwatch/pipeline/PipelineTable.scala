package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
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
                          private val _mode: WriteMode = WriteMode.append,
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
  val tableFullName: String = s"${databaseName}.${name}"

  // Minimum Schema Enforcement Management
  private var withMasterMinimumSchema: Boolean = if (masterSchema.nonEmpty) true else false
  private var enforceNonNullable: Boolean = if (masterSchema.nonEmpty) true else false

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
    if (!exists && _mode == WriteMode.merge) {
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
   * default catalog only validation
   *
   * @return
   */
  def exists: Boolean = {
    //    spark.catalog.tableExists(tableFullName)
    exists()
  }

  /**
   * Does a table exists as defined by
   *
   * @param pathValidation    does the path exist for the source -- even if the catalog table does not
   * @param dataValidation    is data present for this organizationId (workspaceId) in the source
   * @param catalogValidation does the catalog table exist for this source -- even if the path does not or is empty
   * @return
   */
  def exists(pathValidation: Boolean = true, dataValidation: Boolean = false, catalogValidation: Boolean = false): Boolean = {
    var entityExists = true
    if (pathValidation || dataValidation) entityExists = Helpers.pathExists(tableLocation)
    if (catalogValidation) entityExists = spark.catalog.tableExists(tableFullName)
    if (dataValidation) { // if other validation is enabled it must first pass those for this test to be attempted
      // opposite -- when result is empty source data does not exist
      entityExists = entityExists && !spark.read.format("delta").load(tableLocation)
        .filter(col("organization_id") === config.organizationId)
        .isEmpty
    }
    entityExists
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

  private def casedSeqCompare(seq1: Seq[String], s2: String): Boolean = {
    if ( // make lower case if column names are case insensitive
      spark.conf.getOption("spark.sql.caseSensitive").getOrElse("false").toBoolean
    ) seq1.contains(s2) else seq1.exists(s1 => s1.equalsIgnoreCase(s2))
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
      val dfFields = instanceDF.schema.fields
      val cronFields = dfFields.filter(f => casedSeqCompare(cronColumnsNames, f.name))

      if (additionalLagDays > 0) require(
        dfFields.map(_.dataType).contains(DateType) || dfFields.map(_.dataType).contains(TimestampType),
        "additional lag days cannot be used without at least one DateType or TimestampType column in the filterArray")

      val filterMsg = s"FILTERING: ${module.moduleName} using cron columns: ${cronFields.map(_.name).mkString(", ")}"
      logger.log(Level.INFO, filterMsg)
      if (config.debugFlag) println(filterMsg)
      val incrementalFilters = cronFields.map(field => {

        field.dataType match {
          case dt: DateType => {
            if (!partitionBy.map(_.toLowerCase).contains(field.name.toLowerCase)) {
              val errmsg = s"Date filters are inclusive on both sides and are used for partitioning. Date filters " +
                s"should not be used in the Overwatch package alone. Date filters must be accompanied by a more " +
                s"granular filter to utilize df.asIncrementalDF.\nERROR: ${field.name} not in partition columns: " +
                s"${partitionBy.mkString(", ")}"
              throw new IncompleteFilterException(errmsg)
            }

            // Nothing in Overwatch is incremental at the date level thus until date should always be inclusive
            // so add 1 day to follow the rule value >= date && value < date
            // but still keep it inclusive
            val untilDate = PipelineFunctions.addNTicks(module.untilTime.asColumnTS, 1, DateType)
            IncrementalFilter(
              field,
              date_sub(module.fromTime.asColumnTS.cast(dt), additionalLagDays.toInt),
              untilDate.cast(dt)
            )
          }
          case dt: TimestampType => {
            val start = if (additionalLagDays > 0) {
              val epochMillis = module.fromTime.asUnixTimeMilli - (additionalLagDays * 24 * 60 * 60 * 1000L)
              from_unixtime(lit(epochMillis).cast(DoubleType) / 1000.0).cast(dt)
            } else {
              module.fromTime.asColumnTS
            }
            IncrementalFilter(
              field,
              start,
              module.untilTime.asColumnTS
            )
          }
          case dt: LongType => {
            val start = if (additionalLagDays > 0) {
              lit(module.fromTime.asUnixTimeMilli - (additionalLagDays * 24 * 60 * 60 * 1000L)).cast(dt)
            } else {
              lit(module.fromTime.asUnixTimeMilli)
            }
            IncrementalFilter(
              field,
              start,
              lit(module.untilTime.asUnixTimeMilli)
            )
          }
          case _ => throw new UnsupportedTypeException(s"UNSUPPORTED TYPE: An incremental Dataframe was derived from " +
            s"a filter containing a ${field.dataType.typeName} type. ONLY Timestamp, Date, and Long types are supported.")
        }
      })

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
          .queryName(s"StreamTo_${name}")
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

  def getDups(): DataFrame = {
    val df = this.asDF
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

//  def deleteDups(tableFullName: String, keyCols: Array[String], orderByCols: Array[String], tableFilters: Array[Column]): Unit = {
  def deleteDups(tableFilters: Array[Column] = Array()): Unit = {
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

    DeltaTable.forName(tableFullName)
      .as("target")
      .merge(
        dupsToDelete.as("source"), conditionalMatchClause
      )
      .whenMatched
      .delete()
      .execute

    DeltaTable.forName(tableFullName)
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
    val dupsDropped = dupsFound- originalsRecovered
    println(s"DROPPED $dupsDropped duplicate records from $tableFullName")
  }


}
