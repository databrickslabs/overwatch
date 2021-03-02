package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.utils.Frequency.Frequency
import com.databricks.labs.overwatch.utils.{Config, FailedModuleException, Frequency, IncrementalFilter, NoNewDataException, SparkSessionWrapper, UnhandledException, UnsupportedTypeException}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import TransformFunctions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}

// TODO -- Add rules: Array[Rule] to enable Rules engine calculations in the append
//  also add ruleStrateg: Enum(Kill, Quarantine, Ignore) to determine when to require them
//  Perhaps add the strategy into the Rule definition in the Rules Engine
case class PipelineTable(
                          name: String,
                          keys: Array[String],
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
                          optimizeFrequency: Int = 24 * 7,
                          zOrderBy: Array[String] = Array(),
                          vacuum: Int = 24 * 7, // TODO -- allow config overrides -- no vacuum == 0
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

  import spark.implicits._

  private val databaseName = if (_databaseName == "default") config.databaseName else config.consumerDatabaseName

  private val (catalogDB, catalogTable) = if (!config.isFirstRun) {
    val dbCatalog = try {
      Some(spark.sessionState.catalog.getDatabaseMetadata(config.databaseName))
    } catch {
      case e: Throwable => None
    }

    val dbTable = try {
      Some(spark.sessionState.catalog.getTableMetadata(new TableIdentifier(name, Some(config.databaseName))))
    } catch {
      case e: Throwable => None
    }
    (dbCatalog, dbCatalog)
  } else (None, None)

  val tableFullName: String = s"${databaseName}.${name}"

  if (autoOptimize) {
    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    println(s"Setting Auto Optimize for ${name}")
  }
  else spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "false")

  if (autoCompact) {
    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
    println(s"Setting Auto Compact for ${name}")
  }
  else spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "false")

  // Minimum Schema Enforcement Management
  private var withMasterMinimumSchema: Boolean = if (masterSchema.nonEmpty) true else false
  private var enforceNonNullable: Boolean = if (masterSchema.nonEmpty) true else false
  private def emitMissingMasterSchemaMessage: Unit = {
    val msg = s"No Master Schema defined for Table $tableFullName"
    logger.log(Level.ERROR, msg)
    if (config.debugFlag) println(msg)
  }

  private[overwatch] def withMinimumSchemaEnforcement: this.type = withMinimumSchemaEnforcement(true)
  private[overwatch] def withMinimumSchemaEnforcement(value: Boolean): this.type = {
    if (masterSchema.nonEmpty) withMasterMinimumSchema = value
    else emitMissingMasterSchemaMessage // cannot enforce master schema if not defined
    this
  }

  private[overwatch] def enforceNullableRequirements: this.type = enforceNullableRequirements(true)
  private[overwatch] def enforceNullableRequirements(value: Boolean): this.type = {
    if (masterSchema.nonEmpty && exists) enforceNonNullable = value
    else emitMissingMasterSchemaMessage // cannot enforce master schema if not defined
    this
  }

  /**
   * This EITHER appends/changes the spark overrides OR sets them. This can only set spark params if updates
   * are not passed --> setting the spark conf is really mean to be private action
   *
   * @param updates spark conf updates
   */
  private[overwatch] def setSparkOverrides(updates: Map[String, String] = Map()): Unit = {
    if (updates.nonEmpty) {
      currentSparkOverrides = currentSparkOverrides ++ updates
    }
    if (sparkOverrides.nonEmpty && updates.isEmpty) {
      PipelineFunctions.setSparkOverrides(spark, currentSparkOverrides, config.debugFlag)
    }
  }

  def exists: Boolean = {
    spark.catalog.tableExists(tableFullName)
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
          PipelineFunctions.applyFilters(fullDF, config.globalFilters.get)
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

  /**
   * Build 1 or more incremental filters for a dataframe from standard start or aditionalStart less
   * "additionalLagDays" when loading an incremental DF with some front-padding to capture lagging start events
   *
   * @param module            module used for logging and capturing status timestamps by module
   * @param additionalLagDays Front-padded days prior to start
   * @param cronColumnsNames  1 or many incremental columns
   * @return Filtered Dataframe
   */
  def asIncrementalDF(
                       module: Module,
                       cronColumnsNames: Seq[String],
                       additionalLagDays: Int = 0
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
      val cronCols = dfFields.filter(f => cronColumnsNames.contains(f.name))

      if (additionalLagDays > 0) require(
        dfFields.map(_.dataType).contains(DateType) || dfFields.map(_.dataType).contains(TimestampType),
        "additional lag days cannot be used without at least one DateType or TimestampType column in the filterArray")
      val incrementalFilters = cronColumnsNames.map(filterCol => {

        val field = cronCols.filter(_.name == filterCol).head

        val logStatement =
          s"""
             |ModuleID: ${moduleId}
             |ModuleName: ${moduleName}
             |IncrementalColumn: ${field.name}
             |FromTime: ${module.fromTime.asTSString} --> ${module.fromTime.asUnixTimeMilli}
             |UntilTime: ${module.untilTime.asTSString} --> ${module.untilTime.asUnixTimeMilli}
             |""".stripMargin
        if (config.debugFlag) println(logStatement)
        logger.log(Level.INFO, logStatement)

        field.dataType match {
          case _: DateType => {
            IncrementalFilter(
              field.name,
              date_sub(module.fromTime.asColumnTS.cast(field.dataType), additionalLagDays),
              module.untilTime.asColumnTS.cast(field.dataType)
            )
          }
          case _: TimestampType => {
            val start = if (additionalLagDays > 0) {
              val epochMillis = module.fromTime.asUnixTimeMilli - (additionalLagDays * 24 * 60 * 60 * 1000)
              from_unixtime(lit(epochMillis).cast(DoubleType) / 1000).cast(TimestampType)
            } else {
              module.fromTime.asColumnTS
            }
            IncrementalFilter(
              field.name,
              start,
              module.untilTime.asColumnTS
            )
          }
          case _: LongType => {
            IncrementalFilter(field.name,
              lit(module.fromTime.asUnixTimeMilli),
              lit(module.untilTime.asUnixTimeMilli)
            )
          }
          case _ => throw new UnsupportedTypeException(s"UNSUPPORTED TYPE: An incremental Dataframe was derived from " +
            s"a filter containing a ${field.dataType.typeName} type. ONLY Timestamp, Date, and Long types are supported.")
        }
      })

      PipelineFunctions.withIncrementalFilters(instanceDF, module, incrementalFilters, config.globalFilters)
    } else { // Source doesn't exist
      spark.emptyDataFrame
    }
  }

  def writer(df: DataFrame): Any = {
    setSparkOverrides()
    val f = if (config.isLocalTesting && !config.isDBConnect) "parquet" else format
    if (checkpointPath.nonEmpty) {
      val streamWriterMessage = s"DEBUG: PipelineTable - Checkpoint for ${tableFullName} == ${checkpointPath.get}"
      if (config.debugFlag) println(streamWriterMessage)
      logger.log(Level.INFO, streamWriterMessage)
      var streamWriter = df.writeStream.outputMode(mode).format(f).option("checkpointLocation", checkpointPath.get)
        .queryName(s"StreamTo_${name}")
      streamWriter = if (partitionBy.nonEmpty) streamWriter.partitionBy(partitionBy: _*) else streamWriter
      streamWriter = if (mode == "overwrite") streamWriter.option("overwriteSchema", "true")
      else if (enableSchemaMerge && mode != "overwrite")
        streamWriter
          .option("mergeSchema", "true")
      else streamWriter
      streamWriter
    } else {
      var dfWriter = df.write.mode(mode).format(f)
      dfWriter = if (partitionBy.nonEmpty) dfWriter.partitionBy(partitionBy: _*) else dfWriter
      dfWriter = if (mode == "overwrite") dfWriter.option("overwriteSchema", "true")
      else if (enableSchemaMerge && mode != "overwrite")
        dfWriter
          .option("mergeSchema", "true")
      else dfWriter
      dfWriter
    }

    // TODO - Validate proper repartition to minimize files per partition. Could be autoOptimize


  }


}
