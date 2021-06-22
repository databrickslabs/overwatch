package com.databricks.labs.overwatch.utils

import com.amazonaws.services.s3.model.AmazonS3Exception
import com.databricks.labs.overwatch.pipeline.{PipelineFunctions, PipelineTable}
import com.fasterxml.jackson.annotation.JsonInclude.{Include, Value}
import com.fasterxml.jackson.core.io.JsonStringEncoder
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
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
   * Getter for parallelism between 8 and driver cores
   *
   * @return
   *
   * TODO: rename to defaultParallelism
   */
  private def parallelism: Int = {
    Math.min(driverCores, 8)
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
  def parOptimize(tables: Array[PipelineTable], maxFileSizeMB: Int): Unit = {
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 1024 * 1024 * maxFileSizeMB)

    val tablesPar = tables.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    tablesPar.tasksupport = taskSupport

    tablesPar.foreach(tbl => {
      try {
        val zorderColumns = if (tbl.zOrderBy.nonEmpty) s"ZORDER BY (${tbl.zOrderBy.mkString(", ")})" else ""
        val sql = s"""optimize ${tbl.tableFullName} $zorderColumns"""
        println(s"optimizing: ${tbl.tableFullName} --> $sql")
        spark.sql(sql)
        if (tbl.vacuum_H > 0) {
          println(s"vacuuming: ${tbl.tableFullName}, Retention == ${tbl.vacuum_H}")
          spark.sql(s"VACUUM ${tbl.tableFullName} RETAIN ${tbl.vacuum_H} HOURS")
        }
        println(s"Complete: ${tbl.tableFullName}")
      } catch {
        case e: Throwable => println(e.printStackTrace())
      }
    })
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
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
   * @param target target table
   * @param cloudProvider - name of the cloud provider
   */
  private[overwatch] def fastDrop(target: PipelineTable, cloudProvider: String): Unit = {
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
  }

  private def getURI(pathString: String): URI = {
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

}
