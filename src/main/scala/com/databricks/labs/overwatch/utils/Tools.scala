package com.databricks.labs.overwatch.utils

import com.amazonaws.services.s3.model.AmazonS3Exception
import com.databricks.labs.overwatch.env.Workspace
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

import java.io.FileNotFoundException
import com.databricks.labs.overwatch.pipeline.TransformFunctions._
import com.databricks.labs.overwatch.pipeline._
import com.fasterxml.jackson.annotation.JsonInclude.{Include, Value}
import com.fasterxml.jackson.core.io.JsonStringEncoder
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.delta.tables.DeltaTable
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.time.LocalDate
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

// TODO -- Add loggers to objects with throwables
object JsonUtils {

  private val logger: Logger = Logger.getLogger(this.getClass)

  case class JsonStrings(prettyString: String, compactString: String, fromObj: Any) {
    lazy val escapedString = new String(encoder.quoteAsString(compactString))
  }

  private def createObjectMapper(includeNulls: Boolean = false, includeEmpty: Boolean = false): ObjectMapper = {
    val obj = new ObjectMapper()
    obj.registerModule(DefaultScalaModule)
    // order of sets does matter...
    if (!includeNulls) {
      obj.setSerializationInclusion(Include.NON_NULL)
      obj.configOverride(classOf[java.util.Map[String, Object]])
        .setInclude(Value.construct(Include.NON_NULL, Include.NON_NULL))
    }
    if (!includeEmpty) {
      obj.setSerializationInclusion(Include.NON_EMPTY)
      obj.configOverride(classOf[java.util.Map[String, Object]])
        .setInclude(Value.construct(Include.NON_EMPTY, Include.NON_EMPTY))
    }
    obj
  }

  /**
   * Extracts the key and value from Json String.This function only extracts the key and value if jsonString contains only one key and value.
   * If the Json contains more then one key and value then this function will return the first pair of key and value.
   *
   * @param jsonString
   * @return
   */
  private[overwatch] def getJsonKeyValue(jsonString: String): (String, String) = {
    try {
      val mapper = new ObjectMapper()
      val actualObj = mapper.readTree(jsonString);
      val key = actualObj.fields().next().getKey
      val value = actualObj.fields().next().getValue.asText()
      (key, value)
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, s"ERROR: Could not extract key and value from json. \nJSON: $jsonString", e)
        throw e
      }
    }

  }

  private[overwatch] lazy val defaultObjectMapper: ObjectMapper =
    createObjectMapper(includeNulls = true, includeEmpty = true)

  // map of (includeNulls, includeEmpty) to corresponding ObjectMapper
  // we need all combinations because we must not change configuration of already existing objects
  private lazy val mappersMap = Map[(Boolean, Boolean), ObjectMapper](
    (true, true) -> defaultObjectMapper,
    (true, false) -> createObjectMapper(includeNulls = true),
    (false, true) -> createObjectMapper(includeEmpty = true),
    (false, false) -> createObjectMapper()
  )

  private val encoder = JsonStringEncoder.getInstance

  /**
   * Converts json strings to map using default scala module.
   *
   * @param message JSON-formatted string
   * @return
   */
  def jsonToMap(message: String): Map[String, Any] = {
    try {
      // TODO: remove this workaround when we know that new Jobs UI is rolled out everywhere...
      val cleanMessage = StringEscapeUtils.unescapeJson(message)
      defaultObjectMapper.readValue(cleanMessage, classOf[Map[String, Any]])
    } catch {
      case _: Throwable =>
        try {
          defaultObjectMapper.readValue(message, classOf[Map[String, Any]])
        } catch {
          case e: Throwable =>
            logger.log(Level.ERROR, s"ERROR: Could not convert json to Map. \nJSON: $message", e)
            Map("ERROR" -> "")
        }
    }
  }

  /**
   * Take in a case class and output the equivalent json string object with the proper schema. The output is a
   * custom type, "JsonStrings" which includes a pretty print json, a compact string, and a quote escaped string
   * so the json output can be used in any case.
   *
   * @param obj          Case Class instance to be converted to JSON
   * @param includeNulls Whether to include nulled fields in the json output
   * @param includeEmpty Whether to include empty fields in the json output.
   *                     By default, setting includeEmpty to false automatically disables nulls as well
   * @return
   *
   */
  def objToJson(obj: Any, includeNulls: Boolean = false, includeEmpty: Boolean = false): JsonStrings = {
    val objMapper = mappersMap.getOrElse((includeNulls, includeEmpty), defaultObjectMapper)

    JsonStrings(
      objMapper.writerWithDefaultPrettyPrinter.writeValueAsString(obj),
      objMapper.writeValueAsString(obj),
      obj
    )
  }

}

/**
 * Helpers object is used throughout like a utility object.
 */
object Helpers extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val driverCores = java.lang.Runtime.getRuntime.availableProcessors()

  import spark.implicits._

  /**
   * Checks whether the provided String value is numeric.//We also need to check for double/float,
   * ticket TODO#770 has been created for the same.
   *        TODO Unit test case for the function.
   *
   * @param value
   * @return
   */
  def isNumeric(value:String):Boolean={
    value.forall(Character.isDigit)
  }

  /**
   * Getter for parallelism between 8 and driver cores
   *
   * @return
   *
   * TODO: rename to defaultParallelism
   */
  private def parallelism: Int = {
    driverCores
  }

  /**
   * Check whether a path exists
   *
   * @param name file/directory name
   * @return
   */
  def pathExists(name: String): Boolean = {
    val path = new Path(name)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(path)
  }

  /**
   * Check the existence of a path. This input path can also be a regular expression.
   *
   * @param name file/directory name as a regex
   * @return
   */
  def pathPatternExists(name: String): Boolean = {
    val path = new Path(name)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val globPath = fs.globStatus(path)
    if (globPath.isEmpty)
      false
    else
       true
  }


  /**
   * Serialized / parallelized method for rapidly listing paths under a sub directory
   *
   * @param path path to the file/directory
   * @return
   */
  def parListFiles(path: String, conf: SerializableConfiguration): Array[String] = {
    try {
      val fs = new Path(path).getFileSystem(conf.value)
      fs.listStatus(new Path(path)).map(_.getPath.toString)
    } catch {
      case _: Throwable => Array(path)
    }
  }

  /**
   *
   * @param fromDT  inclusive
   * @param untilDT until date is exclusive
   * @return array of strings of dates between fromDT and untilDT in YYYY-mm-dd format
   */
  def getDatesGlob(fromDT: LocalDate, untilDT: LocalDate): Array[String] = {
    datesStream(fromDT).takeWhile(_.isBefore(untilDT)).map(_.toString).toArray
  }

  /**
   * Serializable path expander from wildcard paths. Given an input like /path/to/<asterisk>/wildcards/<asterisk>
   * all paths in that wildcard path will be returned in the array. The key to the performance of this function
   * is ensuring spark is used to serialize it meaning make sure that it's called from the lambda of a Dataset
   *
   * TODO - This function can be easily enhanced to take in String* so that multiple, unrelated wildcards can be
   * globbed simultaneously
   *
   * @param pathString wildcard path as string
   * @return list of all paths contained within the wildcard path
   */

  case class PathStringFileStatus(
                                   pathString: String,
                                   fileCreateEpochMS: Option[Long],
                                   fileSize: Option[Long],
                                   withinSpecifiedTimeRange: Boolean,
                                   failed: Boolean,
                                   failMsg: Option[String]
                                 )

  def globPath(path: String, fromEpochMillis: Option[Long] = None, untilEpochMillis: Option[Long] = None): Array[PathStringFileStatus] = {
    globPath(path, spark.sparkContext.hadoopConfiguration, fromEpochMillis, untilEpochMillis)
  }

  def globPath(path: String, conf: SerializableConfiguration, fromEpochMillis: Option[Long],
               untilEpochMillis: Option[Long]): Array[PathStringFileStatus] = {
    globPath(path, conf.value, fromEpochMillis, untilEpochMillis)
  }

  def globPath(path: String, conf: Configuration, fromEpochMillis: Option[Long], untilEpochMillis: Option[Long]): Array[PathStringFileStatus] = {
    logger.log(Level.DEBUG, s"PATH PREFIX: $path")
    try {
      val fs = new Path(path).getFileSystem(conf)
      val paths = fs.globStatus(new Path(path))
      logger.log(Level.DEBUG, s"$path expanded in ${paths.length} files")
      paths.map(wildString => {
        val path = wildString.getPath
        val pathString = path.toString
        val fileStatusOp = fs.listStatus(path).find(_.isFile)
        if (fileStatusOp.nonEmpty) {
          val fileStatus = fileStatusOp.get
          val lastModifiedTS = fileStatus.getModificationTime
          val debugProofMsg = s"PROOF: $pathString --> ${fromEpochMillis.getOrElse(0L)} <= " +
            s"$lastModifiedTS < ${untilEpochMillis.getOrElse(Long.MaxValue)}"
          logger.log(Level.DEBUG, debugProofMsg)
          val isWithinSpecifiedRange = fromEpochMillis.getOrElse(0L) <= lastModifiedTS &&
            untilEpochMillis.getOrElse(Long.MaxValue) > lastModifiedTS
          PathStringFileStatus(pathString, Some(lastModifiedTS), Some(fileStatus.getLen), isWithinSpecifiedRange, failed = false, None)
        } else {
          val msg = s"Could not retrieve FileStatus for path: $pathString"
          logger.log(Level.ERROR, msg)
          // Return failed if timeframe specified but fileStatus is Empty
          val isFailed = if (fromEpochMillis.nonEmpty || untilEpochMillis.nonEmpty) true else false
          PathStringFileStatus(pathString, None, None, withinSpecifiedTimeRange = false, failed = isFailed, Some(msg))
        }
      })
    } catch {
      case e: AmazonS3Exception =>
        val errMsg = s"ACCESS DENIED: " +
          s"Cluster Event Logs at path $path are inaccessible with given the Databricks account used to run Overwatch. " +
          s"Validate access & try again.\n${e.getMessage}"
        logger.log(Level.ERROR, errMsg)
        Array(PathStringFileStatus(path, None, None, withinSpecifiedTimeRange = false, failed = true, Some(errMsg)))
      case e: Throwable =>
        val msg = s"Failed to retrieve FileStatus for Path: $path. ${e.getMessage}"
        logger.log(Level.ERROR, msg)
        Array(PathStringFileStatus(path, None, None, withinSpecifiedTimeRange = false, failed = true, Some(msg)))
    }
  }

  /**
   * Return tables from a given database. Try to use Databricks' fast version if that fails for some reason, revert
   * back to using standard open source version
   *
   * @param db name of the database
   * @return list of tables in given database
   */
  // TODO: switch to the "SHOW TABLES" instead - it's much faster
  // TODO: also, should be a flag showing if we should omit temporary tables, etc.
  def getTables(db: String): Array[String] = {
    try {
      // TODO: change to spark.sessionState.catalog.listTables(db).map(_.table).toArray
      spark.sessionState.catalog.listTables(db).map(_.table).toArray
    } catch {
      case _: Throwable =>
        // TODO: change to spark.catalog.listTables(db).select("name").as[String].collect()
        spark.catalog.listTables(db).select("name").as[String].collect()
    }
  }

  // TODO -- Simplify and combine the functionality of all three parOptimize functions below.

  /**
   * Parallel optimizer with support for vacuum and zordering. This version of parOptimize will optimize (and zorder)
   * all tables in a Database
   *
   * @param db             Database to optimize
   * @param parallelism    How many tables to optimize at once. Be careful here -- if the parallelism is too high relative
   *                       to the cluster size issues will arise. There are also optimize parallelization configs to take
   *                       into account as well (i.e. spark.databricks.delta.optimize.maxThreads)
   * @param zOrdersByTable Map of tablename -> Array(field names) to be zordered. Order matters here
   * @param vacuum         Whether or not to vacuum the tables
   * @param retentionHrs   Number of hours for retention regarding vacuum. Defaulted to standard 168 hours (7 days) but
   *                       can be overridden. NOTE: the safeguard has been removed here, so if 0 hours is used, no error
   *                       will be thrown.
   */
  def parOptimize(db: String, parallelism: Int = parallelism - 1,
                  zOrdersByTable: Map[String, Array[String]] = Map(),
                  vacuum: Boolean = true, retentionHrs: Int = 168): Unit = {
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1024 * 1024 * 256)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    val tables = getTables(db)
    val tablesPar = tables.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    tablesPar.tasksupport = taskSupport

    tablesPar.foreach(tbl => {
      try {
        val zorderColumns = if (zOrdersByTable.contains(tbl)) s"ZORDER BY (${zOrdersByTable(tbl).mkString(", ")})" else ""
        val sql = s"""optimize $db.$tbl $zorderColumns"""
        println(s"optimizing: $db.$tbl --> $sql")
        spark.sql(sql)
        if (vacuum) {
          println(s"vacuuming: $db.$tbl")
          spark.sql(s"vacuum $db.$tbl RETAIN $retentionHrs HOURS")
        }
        println(s"Complete: $db.$tbl")
      } catch {
        case e: Throwable => println(e.printStackTrace())
      }
    })
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
  }

  /**
   * Same purpose as parOptimize above but instead of optimizing an entire database, only specific tables are
   * optimized.
   *
   * @param tables        Array of Overwatch PipelineTable
   * @param maxFileSizeMB Optimizer's max file size in MB. Default is 1000 but that's too large so it's commonly
   *                      reduced to improve parallelism
   */
  def parOptimize(tables: Array[PipelineTable], maxFileSizeMB: Int, includeVacuum: Boolean): Unit = {
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1024 * 1024 * maxFileSizeMB)

    val tablesPar = tables.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism - 1))
    tablesPar.tasksupport = taskSupport

    tablesPar.foreach(tbl => {
      try {
        val zorderColumns = if (tbl.zOrderBy.nonEmpty) s"ZORDER BY (${tbl.zOrderBy.mkString(", ")})" else ""
        val sql = s"""optimize delta.`${tbl.tableLocation}` $zorderColumns"""
        println(s"optimizing: ${tbl.tableLocation} --> $sql")
        spark.sql(sql)
        if (tbl.vacuum_H > 0 && includeVacuum) {
          println(s"vacuuming: ${tbl.tableLocation}, Retention == ${tbl.vacuum_H}")
          spark.sql(s"VACUUM delta.`${tbl.tableLocation}` RETAIN ${tbl.vacuum_H} HOURS")
        }
        println(s"Complete: ${tbl.tableLocation}")
      } catch {
        case e: Throwable => println(e.printStackTrace())
      }
    })
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
  }

  def parOptimize(tables: Array[PipelineTable], maxFileSizeMB: Int): Unit = {
    parOptimize(tables, maxFileSizeMB, includeVacuum = true)
  }

  /**
   * Simplified version of parOptimize that allows for the input of array of string where the strings are the fully
   * qualified database.tablename
   *
   * @param tables      Fully-qualified database.tablename
   * @param parallelism Number of tables to optimize simultaneously
   */
  def parOptimizeTables(tables: Array[String],
                        parallelism: Int = parallelism - 1): Unit = {
    val tablesPar = tables.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    tablesPar.tasksupport = taskSupport

    tablesPar.foreach(tbl => {
      try {
        println(s"optimizing: $tbl")
        spark.sql(s"optimize $tbl")
        println(s"Complete: $tbl")
      } catch {
        case e: Throwable => println(e.printStackTrace())
      }
    })
  }

  /**
   * drop database cascade / drop table the standard functionality is serial. This function completes the deletion
   * of files in serial along with the call to the drop table command. A faster way to do this is to call truncate and
   * then vacuum to 0 hours which allows for eventual consistency to take care of the cleanup in the background.
   * Be VERY CAREFUL with this function as it's a nuke. There's a different methodology to make this work depending
   * on the cloud platform. At present Azure and AWS are both supported
   *
   * @param target        target table
   * @param cloudProvider - name of the cloud provider
   */
  @throws(classOf[UnhandledException])
  private[overwatch] def fastDrop(target: PipelineTable, cloudProvider: String): String = {
    require(target.exists, s"TARGET DOES NOT EXIST: ${target.tableFullName}")
    spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
    if (cloudProvider == "aws") {
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      spark.sql(s"truncate table ${target.tableFullName}")
      spark.sql(s"VACUUM ${target.tableFullName} RETAIN 0 HOURS")
      spark.sql(s"drop table if exists ${target.tableFullName}")
      fastrm(Array(target.tableLocation))
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    } else {
      Seq("").toDF("HOLD")
        .write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(target.tableFullName)
      spark.sql(s"drop table if exists ${target.tableFullName}")
      fastrm(Array(target.tableLocation))
    }
    spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "false")
    s"SHRED COMPLETE: ${target.tableFullName}"
  }

  /**
   * Execute a parallelized clone to follow the instructions provided through CloneDetail class
   *
   * @param cloneDetails details required to execute the parallelized clone
   * @return
   */
  def parClone(cloneDetails: Seq[CloneDetail]): Seq[CloneReport] = {
    val cloneDetailsPar = cloneDetails.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    cloneDetailsPar.tasksupport = taskSupport

    logger.log(Level.INFO, "CLONE START:")
    cloneDetailsPar.map(cloneSpec => {
      val baseCloneStatement = s"CREATE OR REPLACE TABLE delta.`${cloneSpec.target}` ${cloneSpec.cloneLevel} CLONE " +
        s"delta.`${cloneSpec.source}`"
      val stmt = if (cloneSpec.asOfTS.isEmpty) { // asofTS empty
        baseCloneStatement
      } else { // asofTS provided
        val temporalCloneStatement = s"$baseCloneStatement TIMESTAMP AS OF '${cloneSpec.asOfTS.get}'"
        temporalCloneStatement
      }
      logger.log(Level.INFO, stmt)
      try {
        spark.sql(stmt)
        logger.log(Level.INFO, s"CLONE COMPLETE: ${cloneSpec.source} --> ${cloneSpec.target}")
        CloneReport(cloneSpec, stmt, "SUCCESS")
      } catch {
        case e: Throwable if (e.getMessage.contains("is after the latest commit timestamp of")) => {
          val msg = s"SUCCESS WITH WARNINGS: The timestamp provided, ${cloneSpec.asOfTS.get} " +
            s"resulted in a temporally unsafe exception. Cloned the source without the as of timestamp arg. " +
            s"\nDELTA ERROR MESSAGE: ${e.getMessage()}"
          logger.log(Level.WARN, msg)
          spark.sql(baseCloneStatement)
          CloneReport(cloneSpec, baseCloneStatement, msg)
        }
        case e: Throwable => CloneReport(cloneSpec, stmt, e.getMessage)
      }
    }).toArray.toSeq
  }

  def getLatestTableVersionByPath(spark: SparkSession, tablePath: String): Long = {
    DeltaTable.forPath(spark, tablePath).history(1).select('version).as[Long].head
  }

  def getLatestTableVersionByName(spark: SparkSession, tableName: String): Long = {
    DeltaTable.forName(spark, tableName).history(1).select('version).as[Long].head
  }

  def getURI(pathString: String): URI = {
    val path = PipelineFunctions.cleansePathURI(pathString)
    new URI(path)
  }

  /**
   * Helper private function for fastrm. Enables serialization
   * This version only supports dbfs but s3 is easy to add it just wasn't necessary at the time this was written
   * TODO -- add support for s3/abfs direct paths
   *
   * @param file path to file
   */
  private def rmSer(file: String): Unit = {
    rmSer(file, spark.sparkContext.hadoopConfiguration)
  }

  private def rmSer(file: String, conf: SerializableConfiguration): Unit = {
    rmSer(file, conf.value)
  }

  private def rmSer(file: String, conf: Configuration): Unit = {
    val fsURI = getURI(file)
    val fs = FileSystem.get(fsURI, conf)
    try {
      fs.delete(new Path(file), true)
    } catch {
      case e: Throwable =>
        logger.log(Level.ERROR, s"ERROR: Could not delete file $file, skipping", e)
    }
  }


  /**
   * SERIALIZABLE drop function
   * Drop all files from an array of top-level paths in parallel. Top-level paths can have wildcards.
   * BE VERY CAREFUL with this function, it's a nuke.
   *
   * @param topPaths Array of wildcard strings to act as parent paths. Every path that is returned from the glob of
   *                 globs will be dropped in parallel
   */
  private[overwatch] def fastrm(topPaths: Array[String]): Unit = {
    val conf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    topPaths.map(p => {
      if (p.reverse.head.toString == "/") s"${p}*" else s"${p}/*"
    }).toSeq.toDF("pathsToDrop")
      .as[String]
      .map(p => Helpers.globPath(p, conf, None, None))
      .select(explode('value).alias("pathsToDrop"))
      .select($"pathsToDrop.pathString")
      .as[String]
      .foreach(f => rmSer(f, conf))

    topPaths.foreach(dir => {
      val fsURI = getURI(dir)
      val fs = FileSystem.get(fsURI, conf.value)
      fs.delete(new Path(dir), true)
    })
  }

  /**
   * Simplifies the acquisition of the workspace
   * Requires that the ETLDB exists and has had successful previous runs
   * As of 0.6.0.4
   * Cannot derive schemas < 0.6.0.3
   *
   * @param etlDB Overwatch ETL database
   * @param organization_id Optional - Use only when trying to instantiate remote deployment - org id of remote workspace
   * @param apiUrl Optiona - Use only when trying to instantiate remote deployment apiURL of remote workspace
   * @param successfullOnly Only consider successful runs when looking for latest config
   * @param disableValidations Whether or not to have initializer disable validations
   * @return
   */
  def getWorkspaceByDatabase(
                              etlDB: String,
                              organization_id: Option[String] = None,
                              apiUrl: Option[String] = None,
                              successfullOnly: Boolean = true,
                              disableValidations: Boolean = false
                            ): Workspace = {
    // verify database exists
    val initialCatalog = getCurrentCatalogName(spark)
      val etlDBWithOutCatalog = if(etlDB.contains(".")){
        setCurrentCatalog(spark, etlDB.split("\\.").head)
      etlDB.split("\\.").last
    } else etlDB

    assert(spark.catalog.databaseExists(etlDBWithOutCatalog),
      s"The database provided, $etlDBWithOutCatalog, does not exist.")
    val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(etlDBWithOutCatalog)
    val dbProperties = dbMeta.properties
    val isRemoteWorkspace = organization_id.nonEmpty

    // verify database is owned and managed by Overwatch
    assert(dbProperties.getOrElse("OVERWATCHDB", "FALSE") == "TRUE", s"The database provided," +
      s" $etlDBWithOutCatalog, is not an Overwatch managed Database. Please provide an Overwatch managed database")
    val workspaceID = if (isRemoteWorkspace) organization_id.get else Initializer.getOrgId(apiUrl)

    val statusFilter = if (successfullOnly) 'status === "SUCCESS" else lit(true)

    val latestConfigByOrg = Window.partitionBy('organization_id).orderBy('Pipeline_SnapTS.desc)
    val testConfig = spark.table(s"${etlDBWithOutCatalog}.pipeline_report")
      .filter(statusFilter)
      .withColumn("rnk", rank().over(latestConfigByOrg))
      .withColumn("rn", row_number().over(latestConfigByOrg))
      .filter('rnk === 1 && 'rn === 1)
      .filter('organization_id === workspaceID)
      .select(to_json('inputConfig).alias("compactString"))
      .as[String].first()

    val workspace = if (isRemoteWorkspace) { // single workspace deployment
      Initializer(testConfig, disableValidations = true)
    } else { // multi workspace deployment
      Initializer(
        testConfig,
        disableValidations = disableValidations,
        apiURL = apiUrl,
        organizationID = organization_id
      )
    }

    // set cloud provider for remote workspaces
    if (isRemoteWorkspace && workspace.getConfig.auditLogConfig.rawAuditPath.nonEmpty) {
      workspace.getConfig.setCloudProvider("aws")
    }
    if (isRemoteWorkspace && workspace.getConfig.auditLogConfig.rawAuditPath.isEmpty) {
      workspace.getConfig.setCloudProvider("azure")
    }
    setCurrentCatalog(spark, initialCatalog)
    workspace
  }

  /**
   * Enable Overwatch to retrieve a remote workspace as it's configured on a remote workspace.
   * Key differences in the way this workspace is initialized is 1) all validations are disabled; thus no
   * errors regarding remote keys not being present, etc. and 2) the database is not initialized since it's
   * likely that the user doesn't want to start a new Overwatch database in the local workspace
   * as it's defined in the remote workspace.
   * Lastly, Pipelines that are build from this workspace instance cannot be run as all pipelines built from
   * this workspace will be set to read only since validations have not been executed.
   *
   * @param pipelineReportPath path to remote "pipeline_report" table. Usually some_prefix/global_share/pipeline_report
   * @param workspaceID        A single organization_id that has been run and successfully completed and reported data
   *                           to this pipeline_report output
   * @return
   */

  def getRemoteWorkspaceByPath(pipelineReportPath: String, successfulOnly: Boolean = true,workspaceID: String): Workspace = {

    //val workspaceID = Initializer.getOrgId

  val statusFilter = if (successfulOnly) 'status === "SUCCESS" else lit(true)
    val latestConfigByOrg = Window.partitionBy('organization_id).orderBy('Pipeline_SnapTS.desc)
    val testConfig = spark.read.format("delta").load(pipelineReportPath)
      .filter(statusFilter)
      .withColumn("rnk", rank().over(latestConfigByOrg))
      .withColumn("rn", row_number().over(latestConfigByOrg))
      .filter('rnk === 1 && 'rn === 1)
      .filter('organization_id === workspaceID)
      .select(to_json('inputConfig).alias("compactString"))
      .as[String].first()
    Initializer(testConfig, disableValidations = true, initializeDatabase = false)
  }

  /**
   * Enables users to create a database with all the Overwatch datasets linked to their remote source WITHOUT needing
   * to run Overwatch on the local workspace.
   * Get the remote workspace using 'getRemoteWorkspaceByPath' function, build a localDataTarget to define the local
   * etl and consumer database details
   *
   * @param remoteStoragePrefix remote storage prefix for the remote workspace where overwatch has been deployed
   * @param remoteWorkspaceID   workSpaceID for the remoteworkspace which will be used in getRemoteWorkspaceByPath
   * @param localETLDatabaseName ETLDatabaseName that user want to override. If not provided then etlDatabase name
   *                             from the remoteWorkspace would be used as etlDatabase for current workspace
   * @param localConsumerDatabaseName ConsumerDatabase that user want to override. If not provided then ConsumerDatabase name
   *                             from the remoteWorkspace would be used as ConsumerDatabase for current workspace
   * @param remoteETLDataPathPrefixOverride Param to override StoragePrefix. If not provided then remoteStoragePrefix+"/global_share"
   *                                        would be used as StoragePrefix
   * @param usingExternalMetastore If user using any ExternalMetastore.
   * @param workspacesAllowed  If we want to fill the data for a specific workSpaceID. In that case only ConsumerDB would be
   *                           visible to the customer
   * @return
   */
  def registerRemoteOverwatchIntoLocalMetastore(
                                                 remoteStoragePrefix: String,
                                                 remoteWorkspaceID: String,
                                                 localETLDatabaseName: String = "",
                                                 localConsumerDatabaseName: String = "",
                                                 remoteETLDataPathPrefixOverride: String = "",
                                                 usingExternalMetastore: Boolean = false,
                                                 workspacesAllowed: Array[String] = Array()
                                               ): Seq[WorkspaceMetastoreRegistrationReport] = {

    // Derive eltDatapathPrefix
    val eltDataPathPrefix = if (remoteETLDataPathPrefixOverride == "") {
      remoteStoragePrefix + "/global_share"
    } else remoteETLDataPathPrefixOverride

    // Check whether eltDatapathPrefix Contains PipReport
    val pipReportPath = eltDataPathPrefix+"/pipeline_report"
    try{
      dbutils.fs.ls(s"$pipReportPath/_delta_log").nonEmpty
      logger.log(Level.INFO, s"Overwatch has being deployed with ${pipReportPath} location...proceed")
    }catch {
      case e: FileNotFoundException =>
        val msg = s"Overwatch has not been deployed with ${pipReportPath} location...can not proceed"
        logger.log(Level.ERROR, msg)
        throw new BadConfigException(msg)
    }

    // Derive Remote Workspace
    val remoteWorkspace = Helpers.getRemoteWorkspaceByPath(pipReportPath,successfulOnly= true,remoteWorkspaceID)

    val remoteConfig = remoteWorkspace.getConfig
    val etlDatabaseNameToCreate = if (localETLDatabaseName == "" & !usingExternalMetastore)  {remoteConfig.databaseName} else {localETLDatabaseName}
    val consumerDatabaseNameToCreate = 	if (localConsumerDatabaseName == "" & !usingExternalMetastore) {remoteConfig.consumerDatabaseName} else {localConsumerDatabaseName}
    val LocalWorkSpaceID = Initializer.getOrgId

    val localETLDBPath = if (!usingExternalMetastore ){
      Some(s"${remoteStoragePrefix}/${LocalWorkSpaceID}/${etlDatabaseNameToCreate}.db")
    }else{
      val storagePrefix = remoteETLDataPathPrefixOverride.split("/").dropRight(1).mkString("/")
      Some(s"${storagePrefix}/${LocalWorkSpaceID}/${etlDatabaseNameToCreate}.db")
    }
    val localConsumerDBPath = if (!usingExternalMetastore) {
      Some(s"${remoteStoragePrefix}/${LocalWorkSpaceID}/${consumerDatabaseNameToCreate}.db")
    }else{
      val storagePrefix = remoteETLDataPathPrefixOverride.split("/").dropRight(1).mkString("/")
      Some(s"${storagePrefix}/${LocalWorkSpaceID}/${consumerDatabaseNameToCreate}.db")
    }


    if (usingExternalMetastore){
      try {
        if (localConsumerDatabaseName == localETLDatabaseName) {
          logger.log(Level.INFO, s"ExternalMetastore Flag is true and localConsumerDatabaseName is same as localETLDatabaseName")
        } else {
          throw new BadConfigException(s"ExternalMetastore Flag is true and localConsumerDatabaseName is not same as localETLDatabaseName")
        }
      }catch{
        case e: BadConfigException =>
          val msg = s"TABLE REGISTRATION FAILED: ${e.getMessage}"
          logger.log(Level.ERROR, msg)
          throw new BadConfigException(msg)
      }
    }else{
      logger.log(Level.INFO, s"External Metastore Flag set as ${usingExternalMetastore}")
    }

    //    if (usingExternalMetastore & localConsumerDatabaseName == localETLDatabaseName){
    //      println("localConsumerDatabaseName and localETLDatabaseName both are same...Proceed")
    //    }else{
    //      println("localConsumerDatabaseName and localETLDatabaseName both are Not same...Does not Proceed")
    //    }

    val localDataTarget = DataTarget(
      Some(etlDatabaseNameToCreate), localETLDBPath, Some(eltDataPathPrefix),
      Some(consumerDatabaseNameToCreate), localConsumerDBPath
    )
    val newConfigParams = remoteWorkspace.getConfig.inputConfig.copy(dataTarget = Some(localDataTarget))
    val newConfigArgs = JsonUtils.objToJson(newConfigParams).compactString
    val localTempWorkspace = Initializer(newConfigArgs, disableValidations = true)
    val registrationReport = localTempWorkspace.addToMetastore()
    val b = Bronze(localTempWorkspace, suppressReport = true, suppressStaticDatasets = false)
    val g = Gold(localTempWorkspace, suppressReport = true, suppressStaticDatasets = false)

    b.refreshViews(workspacesAllowed)
    g.refreshViews(workspacesAllowed)
    if (workspacesAllowed.nonEmpty){
      if (spark.catalog.databaseExists(etlDatabaseNameToCreate)) spark.sql(s"Drop Database ${etlDatabaseNameToCreate} cascade")
    }
    registrationReport
  }

  private def rollbackTargetToTimestamp(
                                         targetsToRollbackByTS: Array[TargetRollbackTS],
                                         dryRun: Boolean
                                       ): Unit = {
    val deleteLogger = Logger.getLogger("ROLLBACK Logger")
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism - 1))
    val targetsToRollback = targetsToRollbackByTS.par
    targetsToRollback.tasksupport = taskSupport

    targetsToRollback.foreach(rollbackTarget => {
      val target = rollbackTarget.target
      val rollbackToTime = Pipeline.createTimeDetail(rollbackTarget.rollbackTS)
      val targetSchema = target.asDF.schema
      val incrementalFields = targetSchema.filter(f => target.incrementalColumns.map(_.toLowerCase).contains(f.name.toLowerCase))
      val incrementalFilters = incrementalFields.map(f => {
        f.dataType.typeName match {
          case "long" => s"${f.name} >= ${rollbackToTime.asUnixTimeMilli}"
          case "double" => s"""cast(${f.name} as long) >= ${rollbackToTime.asUnixTimeMilli}"""
          case "date" => s"${f.name} >= '${rollbackToTime.asDTString}'"
          case "timestamp" => s"${f.name} >= '${rollbackToTime.asTSString}'"
        }
      })
      val orgIdFilter = s" and organization_id = '${rollbackTarget.organization_id}'"
      val deleteClause = incrementalFilters.reduce((x, y) => s"$x and $y ") + orgIdFilter
      val deleteStatement =
        s"""
           |delete from ${target.tableFullName}
           |where $deleteClause
           |""".stripMargin
      deleteLogger.info(s"DELETE STATEMENT: $deleteStatement")
      try {
        if (!dryRun) spark.sql(deleteStatement)
      } catch {
        case e: Throwable =>
          val failMsg = s"FAILED DELETE FROM TARGET: ${target.tableFullName}\n\nDELETE STATEMENT: ${deleteStatement}"
          println(PipelineFunctions.appendStackStrace(e, failMsg))
          deleteLogger.error(PipelineFunctions.appendStackStrace(e, failMsg))
      }
    })

  }

  private def rollbackPipelineStateToTimestamp(
                                                rollbackTSByModule: Array[ModuleRollbackTS],
                                                customRollbackStatus: String,
                                                config: Config,
                                                dryRun: Boolean
                                              ): Unit = {
    val rollbackLogger = Logger.getLogger("Overwatch_State: ROLLBACK Logger")

    rollbackTSByModule.foreach(rollbackDetail => {
      val updateClause =
        s"""
           |update ${config.databaseName}.pipeline_report
           |set status = concat('$customRollbackStatus', ' - ', status)
           |where organization_id = '${rollbackDetail.organization_id}'
           |and fromTS >= ${rollbackDetail.rollbackTS}
           |and moduleId = ${rollbackDetail.moduleId}
           |""".stripMargin
      rollbackLogger.info(updateClause)
      try {
        if (!dryRun) spark.sql(updateClause)
      } catch {
        case e: Throwable =>
          val failMsg = s"FAILED TARGET STATE UPDATE:\n MODULE ID: " +
            s"${rollbackDetail.moduleId}\nRollbackTS: ${rollbackDetail.rollbackTS}\nORGID: " +
            s"${rollbackDetail.organization_id}"
          println(PipelineFunctions.appendStackStrace(e, failMsg))
          rollbackLogger.error(PipelineFunctions.appendStackStrace(e, failMsg))
      }
    })
  }

  def rollbackPipelineForModule(
                                 workspace: Workspace,
                                 rollbackToTimeEpochMS: Long,
                                 moduleIds: Array[Int],
                                 workspaceIds: Array[String],
                                 dryRun: Boolean = true,
                                 customRollbackStatus: String = "ROLLED BACK"
                               ): Unit = {
    if (dryRun) println("DRY RUN: Nothing will be changed")
    val config = workspace.getConfig
    val orgFilter = if (workspaceIds.isEmpty) {
      throw new Exception("""workspaceIDs cannot be empty. To rollback all workspaces use "global" in the array """)
    } else if (workspaceIds.headOption.getOrElse(config.organizationId) == "global") {
      lit(true)
    } else {
      'organization_id.isin(workspaceIds: _*)
    }
    val latestRunW = Window.partitionBy('organization_id, 'moduleId).orderBy('fromTS)

    val rollbackTSByModule = spark.table(s"${config.databaseName}.pipeline_report")
      .filter(orgFilter)
      .filter('moduleId.isin(moduleIds: _*))
      .filter('untilTS >= rollbackToTimeEpochMS)
      .withColumn("rnk", rank().over(latestRunW))
      .withColumn("rn", row_number().over(latestRunW))
      .filter('rnk === 1 && 'rn === 1)
      .select('organization_id, 'moduleId, 'fromTS.alias("rollbackTS"))
      .as[ModuleRollbackTS]
      .collect()

    println(s"BEGINNING PIPELINE STATE ROLLBACK for modules " +
      s"${rollbackTSByModule.map(_.moduleId).distinct.mkString(", ")} for ORGANIZATION IDs " +
      s"${rollbackTSByModule.map(_.organization_id).distinct.mkString(", ")}")
    rollbackPipelineStateToTimestamp(rollbackTSByModule, customRollbackStatus, config, dryRun)

    val allTargets = (Bronze(workspace, suppressReport = true, suppressStaticDatasets = true).getAllTargets ++
      Silver(workspace, suppressReport = true, suppressStaticDatasets = true).getAllTargets ++
      Gold(workspace, suppressReport = true, suppressStaticDatasets = true).getAllTargets)
        .filter(_.exists(pathValidation = false, catalogValidation = true))

    val targetsToRollback = rollbackTSByModule.map(rollback => {
      val targetTableName = PipelineFunctions.getTargetTableNameByModule(rollback.moduleId)
      val targetToRollback = allTargets.find(_.name.toLowerCase == targetTableName.toLowerCase)
      assert(targetToRollback.nonEmpty, s"Target with name: $targetTableName not found")
      TargetRollbackTS(
        rollback.organization_id,
        targetToRollback.get,
        rollback.rollbackTS
      )
    })

    println(s"BEGINNING TARGET ROLLBACK FOR TABLES " +
      s"${targetsToRollback.map(_.target.tableFullName).distinct.mkString(", ")} for WORKSPACE IDs " +
      s"${targetsToRollback.map(_.organization_id).distinct.mkString(", ")}"
    )
    rollbackTargetToTimestamp(targetsToRollback, dryRun)

  }

  private def buildPipReport(
                              etlDB: String,
                              noOfRecords: Int,
                              orgIdFilter: Column,
                              moduleIDFilter: Column,
                              verbose: Boolean = false
                            ): DataFrame = {
    try{
      spark.catalog.getTable(s"${etlDB}.pipeline_report")
      logger.log(Level.INFO, s"Overwatch has being deployed with  ${etlDB}")
    }catch {
      case e: Exception =>
        val msg = s"Overwatch has not been deployed with  ${etlDB}"
        logger.log(Level.ERROR, msg)
        throw new BadConfigException(msg)
    }

    val pipReport = spark.table(s"${etlDB}.pipeline_report")
      .filter(orgIdFilter)
      .filter(moduleIDFilter)
    val priorityFields = Array("organization_id", "workspace_name", "moduleID", "moduleName", "from_time",
      "until_time", "primordialDateString", "status", "parsedConfig.packageVersion",
      "Pipeline_SnapTS", "Overwatch_RunID")
    val baseReport = pipReport
      .orderBy('Pipeline_SnapTS.desc,'moduleID)
      .withColumn("from_time", from_unixtime('fromTS.cast("double") / lit(1000)).cast("timestamp"))
      .withColumn("until_time", from_unixtime('untilTS.cast("double") / lit(1000)).cast("timestamp"))

    val organizedReport = if (verbose) {
      baseReport
        .moveColumnsToFront(priorityFields)
    } else {
      baseReport.select(priorityFields map col: _*)
    }
    if (noOfRecords == -1){
      organizedReport
    }else{
      val w = Window.partitionBy('organization_id, 'moduleID).orderBy('Pipeline_SnapTS.desc)
      organizedReport
        .withColumn("rnk", rank().over(w))
        .filter('rnk <= noOfRecords)
        .drop("rnk")
    }

  }



  def pipReport(
                 etlDB: String,
                 noOfRecords:Int = -1,
                 moduleIds: Array[Int] = Array[Int](),
                 verbose: Boolean = false,
                 orgId: Seq[String] = Seq[String]()
               ): DataFrame = {
    val orgIDFilter = if (orgId.nonEmpty) col("organization_id").isin(orgId: _*) else lit(true)
    val moduleIdFilter = if (moduleIds.nonEmpty) col("moduleId").isin(moduleIds: _*) else lit(true)
    buildPipReport(etlDB, noOfRecords, orgIDFilter, moduleIdFilter, verbose)
  }

  def pipReport(etlDB: String, orgId: String*): DataFrame = {
    pipReport(etlDB, orgId = orgId)
  }

  def pipReport(etlDB: String, moduleIds: Array[Int], orgId: String*): DataFrame = {
    pipReport(etlDB, moduleIds = moduleIds, orgId = orgId)
  }
  /**
   * Function removes the trailing slashes and double slashes of the given URL.
   * @param url
   * @return
   */
  def sanitizeURL(url:String):String={
    val inputUrl = url.trim
    removeDuplicateSlashes(removeTrailingSlashes(inputUrl))
  }

  /**
   * FUnction removes the double slashes of the given URL.
   * @param url
   * @return
   */
  def removeDuplicateSlashes(url: String): String = {
    val stringURL = url.replaceAll("//", "/")
    val makeFirstSlashDoubleSlash =
      if (stringURL.contains("s3a:/") ||
        stringURL.contains("s3:/") ||
        stringURL.contains("gs:/") ||
        stringURL.contains("abfss:/") ||
        stringURL.contains("http:/") ||
        stringURL.contains("https:/")) true else false
    if (makeFirstSlashDoubleSlash) {
      stringURL.replaceFirst("/", "//")
    } else {
      stringURL
    }
  }

  /**
   * Removes the slash if the slash is  is present at the end of the URL.
   * @param url
   * @return
   */
  def removeTrailingSlashes(url: String): String = {
    if(url.lastIndexOf("/") == url.length-1){
      url.substring(0,url.length-1)
    }else{
      url
    }
  }

  /**
   * Removes the slash if the slash is  is present at the end of the URL.
   *
   * @param url
   * @return
   */
  def removeTrailingSlashes(url: Column): Column = {
    when(url.endsWith("/"), url.substr(lit(0), length(url) - 1)).otherwise(url)
  }

}
