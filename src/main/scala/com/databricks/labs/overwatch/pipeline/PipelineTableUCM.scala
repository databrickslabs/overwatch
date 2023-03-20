package com.databricks.labs.overwatch.pipeline

import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.utils.WriteMode.{WriteMode, overwrite}
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
class PipelineTableUCM(
                          name: String,
                          private val _keys: Array[String],
                          config: Config,
                          incrementalColumns: Array[String] = Array(),
                          format: String = "delta", // TODO -- Convert to Enum
                          persistBeforeWrite: Boolean = false,
                          _mode: WriteMode = WriteMode.append,
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
                        ) extends PipelineTable( name: String,
                                                 _keys: Array[String],
                                                  config: Config,
                                                  incrementalColumns = Array(),
                                                  format = "delta", // TODO -- Convert to Enum
                                                  persistBeforeWrite = false,
                                                  _mode = WriteMode.append,
                                                  maxMergeScanDates = 33, // used to create explicit date merge condition -- should be removed after merge dynamic partition pruning is enabled DBR 11.x LTS
                                                   _permitDuplicateKeys = true,
                                                   _databaseName = "default",
                                                  autoOptimize = false,
                                                  autoCompact = false,
                                                  partitionBy = Seq(),
                                                  statsColumns = Array(),
                                                  optimizeFrequency_H = 24 * 7,
                                                  zOrderBy = Array(),
                                                  vacuum_H = 24 * 7, // TODO -- allow config overrides -- no vacuum == 0
                                                  enableSchemaMerge = true,
                                                  withCreateDate = true,
                                                  withOverwatchRunID = true,
                                                  workspaceName = true,
                                                  isTemp = false,
                                                  checkpointPath = None,
                                                  masterSchema = None)
  with SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

//  val databaseName: String = if (_databaseName == "default") config.databaseName else _databaseName
  override val tableFullName: String = s"${config.catalogName}.${databaseName}.${name}"


  override def writeMode: WriteMode = { // initialize to constructor value
    if (!exists(dataValidation = true) && _mode == WriteMode.merge) {
      val onetimeModeChangeMsg = s"MODE CHANGED from MERGE to APPEND. Target ${tableFullName} does not exist, first write will be " +
        s"performed as an append, subsequent writes will be written as merge to this target"
      if (config.debugFlag) println(onetimeModeChangeMsg)
      logger.log(Level.INFO, onetimeModeChangeMsg)
      WriteMode.append
    } else _mode
  }

  /**
   * Does a table exists as defined by
   *
   * @param pathValidation    does the path exist for the source -- even if the catalog table does not
   * @param dataValidation    is data present for this organizationId (workspaceId) in the source
   * @param catalogValidation does the catalog table exist for this source -- even if the path does not or is empty
   * @return
   */
  override def exists(pathValidation: Boolean = true, dataValidation: Boolean = false, catalogValidation: Boolean = false): Boolean = {
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

  override def asDF(withGlobalFilters: Boolean = true): DataFrame = {
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
  override def asIncrementalDF(
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



  override def writer(df: DataFrame): Any = { // TODO -- don't return any, return instance of trait
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
}