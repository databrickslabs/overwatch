package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame}

// TODO -- Add rules: Array[Rule] to enable Rules engine calculations in the append
//  also add ruleStrategy: Enum(Kill, Quarantine, Ignore) to determine when to require them
//  Perhaps add the strategy into the Rule definition in the Rules Engine
case class PipelineTable(
                          name: String,
                          private val _keys: Array[String],
                          config: Config,
                          incrementalColumns: Array[String] = Array(),
                          dataFrequency: Frequency = Frequency.milliSecond,
                          format: String = "delta", // TODO -- Convert to Enum
                          mode: String = "append", // TODO -- Convert to Enum
                          _databaseName: String = "default",
                          autoOptimize: Boolean = false,
                          autoCompact: Boolean = false,
                          partitionBy: Seq[String] = Seq(),
                          statsColumns: Array[String] = Array(),
                          shuffleFactor: Double = 1.0,
                          optimizeFrequency_H: Int = 24 * 7,
                          zOrderBy: Array[String] = Array(),
                          vacuum_H: Int = 24 * 7, // TODO -- allow config overrides -- no vacuum == 0
                          enableSchemaMerge: Boolean = true,
                          sparkOverrides: Map[String, String] = Map[String, String](),
                          withCreateDate: Boolean = true,
                          withOverwatchRunID: Boolean = true,
                          isTemp: Boolean = false,
                          checkpointPath: Option[String] = None,
                          masterSchema: Option[StructType] = None
                        ) extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private var currentSparkOverrides: Map[String, String] = sparkOverrides
  logger.log(Level.INFO, s"Spark Overrides Initialized for target: ${_databaseName}.${name} to\n${currentSparkOverrides.mkString(", ")}")

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

  /**
   * This EITHER appends/changes the spark overrides OR sets them. This can only set spark params if updates
   * are not passed --> setting the spark conf is really mean to be private action
   *
   * @param updates spark conf updates
   */
  private[overwatch] def applySparkOverrides(updates: Map[String, String] = Map()): Unit = {

    if (autoOptimize) {
      logger.log(Level.INFO, s"enabling optimizeWrite on $tableFullName")
      spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
      if (config.debugFlag) println(s"Setting Auto Optimize for ${name}")
    }
    else spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "false")

    if (autoCompact) {
      logger.log(Level.INFO, s"enabling autoCompact on $tableFullName")
      spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
      if (config.debugFlag) println(s"Setting Auto Compact for ${name}")
    }
    else spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "false")

    if (updates.nonEmpty) {
      currentSparkOverrides = currentSparkOverrides ++ updates
    }

    if (currentSparkOverrides.nonEmpty) {
      PipelineFunctions.setSparkOverrides(spark, currentSparkOverrides, config.debugFlag)
    } else {
      logger.log(Level.INFO, s"USING spark conf defaults for $tableFullName")
    }
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

  def exists: Boolean = {
    spark.catalog.tableExists(tableFullName)
  }

  def keys: Array[String] = keys()
  def keys(withOveratchMeta: Boolean = false): Array[String] = {
    if (withOveratchMeta) {
      (_keys :+ "organization_id") ++ Array("Overwatch_RunID", "Pipeline_SnapTS")
    } else {
      _keys :+ "organization_id"
    }
  }

  def asDF: DataFrame = {
    asDF()
  }

  def asDF(withGlobalFilters: Boolean = true): DataFrame = {
    try {
      if (exists) {
        val fullDF = if (withMasterMinimumSchema) { // infer master schema if true and available
          logger.log(Level.INFO, s"SCHEMA -> Minimum Schema enforced for $tableFullName")
          spark.table(tableFullName).verifyMinimumSchema(masterSchema, enforceNonNullable, config.debugFlag)
        } else spark.table(tableFullName)
        if (withGlobalFilters && config.globalFilters.nonEmpty)
          PipelineFunctions.applyFilters(fullDF, config.globalFilters, config.debugFlag)
        else fullDF
      } else spark.emptyDataFrame
    } catch {
      case e: AnalysisException =>
        logger.log(Level.WARN, s"WARN: ${tableFullName} does not exist or cannot apply global filters. " +
          s"Will attempt to continue", e)
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

    if (exists) {
      val instanceDF = if (withMasterMinimumSchema) { // infer master schema if true and available
        logger.log(Level.INFO, s"SCHEMA -> Minimum Schema enforced for Module: " +
          s"$moduleId --> $moduleName for Table: $tableFullName")
        spark.table(tableFullName).verifyMinimumSchema(masterSchema,enforceNonNullable, config.debugFlag)
      } else spark.table(tableFullName)
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
            // If Overwatch runs > 1X / day the date filter must be inclusive or filter will omit all data since
            // the following cannot be true
            // date >= today && date < today
            // The above right-side exclusive filter is created from the filter generator thus one tick must be
            // added to the right (end) date
            val untilDate = if (module.fromTime.asLocalDateTime.toLocalDate == module.untilTime.asLocalDateTime.toLocalDate) {
              PipelineFunctions.addNTicks(module.untilTime.asColumnTS, 1, DateType)
            } else module.untilTime.asColumnTS
            IncrementalFilter(
              field,
              date_sub(module.fromTime.asColumnTS.cast(dt), additionalLagDays.toInt),
              untilDate.cast(dt)
            )
          }
          case dt: TimestampType => {
            val start = if (additionalLagDays > 0) {
              val epochMillis = module.fromTime.asUnixTimeMilli - (additionalLagDays * 24 * 60 * 60 * 1000)
              from_unixtime(lit(epochMillis).cast(DoubleType) / 1000).cast(dt)
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
              lit(module.fromTime.asUnixTimeMilli - (additionalLagDays * 24 * 60 * 60 * 1000)).cast(dt)
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
        instanceDF, Some(module), incrementalFilters, config.globalFilters, dataFrequency, config.debugFlag
      )
    } else { // Source doesn't exist
      spark.emptyDataFrame
    }
  }

  def writer(df: DataFrame): Any = {
    if (checkpointPath.nonEmpty) {
      val streamWriterMessage = s"DEBUG: PipelineTable - Checkpoint for ${tableFullName} == ${checkpointPath.get}"
      if (config.debugFlag) println(streamWriterMessage)
      logger.log(Level.INFO, streamWriterMessage)
      var streamWriter = df.writeStream.outputMode(mode).format(format).option("checkpointLocation", checkpointPath.get)
        .queryName(s"StreamTo_${name}")
      streamWriter = if (partitionBy.nonEmpty) streamWriter.partitionBy(partitionBy: _*) else streamWriter
      streamWriter = if (mode == "overwrite") streamWriter.option("overwriteSchema", "true")
      else if (enableSchemaMerge && mode != "overwrite")
        streamWriter
          .option("mergeSchema", "true")
      else streamWriter
      streamWriter
    } else {
      var dfWriter = df.write.mode(mode).format(format)
      dfWriter = if (partitionBy.nonEmpty) dfWriter.partitionBy(partitionBy: _*) else dfWriter
      dfWriter = if (mode == "overwrite") dfWriter.option("overwriteSchema", "true")
      else if (enableSchemaMerge && mode != "overwrite")
        dfWriter
          .option("mergeSchema", "true")
      else dfWriter
      dfWriter
    }

  }


}
