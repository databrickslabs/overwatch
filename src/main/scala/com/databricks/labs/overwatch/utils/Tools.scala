package com.databricks.labs.overwatch.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.core.io.JsonStringEncoder
import java.util.{Date, UUID}

import com.databricks.labs.overwatch.pipeline.PipelineTable
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.conf._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import javax.crypto
import javax.crypto.KeyGenerator
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec}
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

// TODO -- Add loggers to objects with throwables
object JsonUtils {

  private val logger: Logger = Logger.getLogger(this.getClass)
  case class JsonStrings(prettyString: String, compactString: String, escapedString: String, fromObj: Any)

  private[overwatch] lazy val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  private val encoder = JsonStringEncoder.getInstance

  /**
   * Converts json strings to map using default scala module.
   * @param message JSON-formatted string
   * @return
   */
  def jsonToMap(message: String): Map[String, Any] = {
    try {
      val cleanMessage = StringEscapeUtils.unescapeJson(message)
      objectMapper.readValue(cleanMessage, classOf[Map[String, Any]])
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, s"ERROR: Could not convert json to Map. \nJSON: ${message}", e)
        Map("ERROR" -> "")
      }
    }
  }

  /**
   * Take in a case class and output the equivalent json string object with the proper schema. The output is a
   * custom type, "JsonStrings" which includes a pretty print json, a compact string, and a quote escaped string
   * so the json output can be used in any case.
   * @param obj Case Class instance to be converted to JSON
   * @param includeNulls Whether to include nulled fields in the json output
   * @param includeEmpty Whether to include empty fields in the json output
   * @return
   *
   * TODO: refactor it - we shouldn't change options after use, and they are not working anyway
   */
  def objToJson(obj: Any, includeNulls: Boolean = false, includeEmpty: Boolean = false): JsonStrings = {
    if (!includeNulls) objectMapper.setSerializationInclusion(Include.NON_NULL)
    if (!includeEmpty) objectMapper.setSerializationInclusion(Include.NON_EMPTY)
    val defaultString = objectMapper.writeValueAsString(obj)
    JsonStrings(
      objectMapper.writerWithDefaultPrettyPrinter.writeValueAsString(obj),
      defaultString,
      new String(encoder.quoteAsString(defaultString)),
      obj
    )
  }

}

/**
 * This entire class was created in an attempt to ensure all API keys remain encrypted between module and that
 * no keys were ever logged. This was abandoned due to some errors but should be revisited. This code should either
 * be removed (along with removed from the APIEnv structure) after it's confirmed that the API key is not stored
 * anywhere in clear text in logs or otherwise.
 * @param key
 */
class Cipher(key: String) {

  private val salt = Array[Byte](16)
  private val spec = new PBEKeySpec(key.toCharArray, salt, 1000, 128 * 8)
  private val keyGenner = KeyGenerator.getInstance("AES")
  keyGenner.init(128)
  private val aesKey = keyGenner.generateKey()
  private val iv = new IvParameterSpec("0102030405060708".getBytes("UTF-8"))
  private val cipher = crypto.Cipher.getInstance("AES/CBC/PKCS5Padding")

  private[overwatch] def encrypt(text: String): Array[Byte] = {
    //    val aesKey = new SecretKeySpec(tempKey.getBytes(), "AES")
    //    val aesKey = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1").generateSecret(spec)
    cipher.init(crypto.Cipher.ENCRYPT_MODE, aesKey, iv)
    cipher.doFinal(text.getBytes("UTF-8"))
  }

  private[overwatch] def decrypt(byteStream: Array[Byte]): String = {
    cipher.init(crypto.Cipher.DECRYPT_MODE, aesKey, iv)
    new String(cipher.doFinal(byteStream))
  }

}

/**
 * SchemaTools is one of the more complext objects in Overwatch as it handles the schema (or lack there of rather)
 * evolution, oddities, and edge cases found when working with the event / audit logs. As of the 0.2 version, there's
 * still significant room for improvement here but it seems to be handling the challenges for now.
 *
 * Spark in general ignored column name case, but delta does not. Delta will throw an error if a dataframe has two
 * columns with the same name but where the name has a different case. This is not well-handled here and should be
 * added.
 */
object SchemaTools extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def structToMap(df: DataFrame, colToConvert: String): Seq[Column] = {
    val schema = df.select(s"${colToConvert}.*").schema
    var mapCols = collection.mutable.LinkedHashSet[Column]()
    schema.fields.foreach(field => {
      mapCols.add(lit(field.name))
      mapCols.add(col(s"${colToConvert}.${field.name}"))
    })
    mapCols.toSeq
  }

  // TODO -- Delta writer is schema case sensitive and will fail on write if column case is not identical on both sides
  //  As such, schema case sensitive validation needs to be enabled and a handler for whether to assume the same data
  //  and merge the data, or drop it, or quarantine it or what. This is very common in cases where a column is of
  //  struct type but the key's are derived via user-input (i.e. event log "properties" field).
  // TODO -- throw exception if the resulting string is empty
  /**
   * Remove special characters from the field name
   * @param s
   * @return
   */
  private def sanitizeFieldName(s: String): String = {
    s.replaceAll("[^a-zA-Z0-9_]", "")
  }

  /**
   * Clean field name and recurse
   * @param field
   * @return
   */
  private def sanitizeFields(field: StructField): StructField = {
    field.copy(name = sanitizeFieldName(field.name), dataType = sanitizeSchema(field.dataType))
  }

  /**
   * When working with complex, evolving schemas across MANY versions and platforms, it's common to wind up with bad
   * schemas. At times schemas have the same name multiple times which cannot be saved. We cannot have Overwatch break
   * due to one bad record in a run, so instead, we add a unique suffix to the end of the offending columns and log
   * / note the issue as a warning as well as print it out in the run log via stdOUT.
   * @param fields
   * @return
   */
  private def generateUniques(fields: Array[StructField]): Array[StructField] = {
    val r = new scala.util.Random(42L) // Using seed to reuse suffixes on continuous duplicates
    val fieldNames = fields.map(_.name.trim.toLowerCase())
    val dups = fieldNames.diff(fieldNames.distinct)
    val dupCount = dups.length
    if (dupCount == 0) {
      fields
    } else {
      val warnMsg = s"WARNING: SCHEMA ERROR --> The following fields were found to be duplicated in the schema. " +
        s"The fields have been renamed in place and should be reviewed.\n" +
        s"DUPLICATE FIELDS:\n" +
        s"${dups.mkString("\n")}"
      println(warnMsg)
      logger.log(Level.WARN, warnMsg)
      val uniqueSuffixes = (0 to fields.length + 10).map(_ => r.alphanumeric.take(6).mkString("")).distinct
      fields.zipWithIndex.map(f => {
        if (dups.contains(f._1.name.toLowerCase())) {
          val generatedUniqueName = f._1.name + "_" + uniqueSuffixes(f._2)
          val uniqueColumnMapping = s"\n${f._1.name} --> ${generatedUniqueName}"
          println(uniqueColumnMapping)
          logger.log(Level.WARN, uniqueColumnMapping)
          f._1.copy(name = generatedUniqueName)
        }
        else f._1
      })
    }
  }

  /**
   * Recursive function to drill into the schema. Currently only supports recursion through structs and array.
   * TODO -- add support for recursion through Maps
   * @param dataType
   * @return
   */
  private def sanitizeSchema(dataType: DataType): DataType = {
    dataType match {
      case dt: StructType =>
        val dtStruct = dt.asInstanceOf[StructType]
        dtStruct.copy(fields = generateUniques(dtStruct.fields.map(sanitizeFields)))
      case dt: ArrayType =>
        val dtArray = dt.asInstanceOf[ArrayType]
        dtArray.copy(elementType = sanitizeSchema(dtArray.elementType))
      case _ => dataType
    }
  }

  /**
   * Main function for cleaning a schema. The point is to remove special characters and duplicates all the way down
   * into the Arrays / Structs.
   * TODO -- Add support for map type recursion cleansing
   * @param df Input dataframe to be cleansed
   * @return
   */
  def scrubSchema(df: DataFrame): DataFrame = {
    spark.createDataFrame(df.rdd, SchemaTools.sanitizeSchema(df.schema).asInstanceOf[StructType])
  }

  /**
   * Delta, by default, calculates statistics on the first 32 columns and there's no way to specify which columns
   * on which to calc stats. Delta can be configured to calc stats on less than 32 columns but it still starts
   * from left to right moving to the nth position as configured. This simplifies the migration of columns to the
   * front of the dataframe to allow them to be "indexed" in front of others.
   *
   * TODO -- Validate order of columns in Array matches the order in the dataframe after the function call.
   *  If input is Array("a", "b", "c") the first three columns should match that order. If it's backwards, the
   *  array should be reversed before progressing through the logic
   * TODO -- change colsToMove to the Seq[String]....
   * TODO: checks for empty list, for existence of columns, etc.
   * @param df Input dataframe
   * @param colsToMove Array of column names to be moved to front of schema
   * @return
   */
  def moveColumnsToFront(df: DataFrame, colsToMove: Array[String]): DataFrame = {
    val allNames = df.schema.names
    val newColumns = (colsToMove ++ (allNames.diff(colsToMove))).map(col)
    df.select(newColumns: _*)
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
   * @return
   *
   * TODO: rename to defaultParallelism
   */
  private def parallelism: Int = {
    Math.min(driverCores, 8)
  }


  /**
   * Generates a complex time struct to simplify time conversions.
   * TODO - Currently ony supports input as a unix epoch time in milliseconds, check for column input type
   *  and support non millis (Long / Int / Double / etc.)
   *  This function should also support input column types of timestamp and date as well for robustness
   * @param start LongType input as a column
   * @param end TimestampType input as a column
   * @return
   */
  def SubtractTime(start: Column, end: Column): Column = {
    val runTimeMS = end - start
    val runTimeS = runTimeMS / 1000
    val runTimeM = runTimeS / 60
    val runTimeH = runTimeM / 60
    struct(
      start.alias("startEpochMS"),
      from_unixtime(start / 1000).cast("timestamp").alias("startTS"),
      end.alias("endEpochMS"),
      from_unixtime(end / 1000).cast("timestamp").alias("endTS"),
      lit(runTimeMS).alias("runTimeMS"),
      lit(runTimeS).alias("runTimeS"),
      lit(runTimeM).alias("runTimeM"),
      lit(runTimeH).alias("runTimeH")
    ).alias("RunTime")
  }

  // TODO -- This is broken -- It looks like I may have to pass in the df.schema as well
  //
  //  def getLocalTime(ts: Column, tz: String): Column = {
  //    ts.expr.dataType match {
  //      case _: TimestampType => from_utc_timestamp(ts, tz)
  //      case _: LongType => from_utc_timestamp(from_unixtime(ts.cast("double") / 1000).cast("timestamp"), tz)
  //      case _: DoubleType => from_utc_timestamp(from_unixtime(ts).cast("timestamp"), tz)
  //      case _: IntegerType => from_utc_timestamp(from_unixtime(ts).cast("timestamp"), tz)
  //    }
  //  }

  /**
   * Check whether a path exists
   * @param path
   * @return
   */
  def pathExists(path: String): Boolean = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.exists(new Path(path))
  }

  /**
   * Serializable path expander from wildcard paths. Given an input like /path/to/<asterisk>/wildcards/<asterisk>
   * all paths in that wildcard path will be returned in the array. The key to the performance of this function
   * is ensuring spark is used to serialize it meaning make sure that it's called from the lambda of a Dataset
   *
   * TODO - This function can be easily enhanced to take in String* so that multiple, unrelated wildcards can be
   *  globbed simultaneously
   * @param path wildcard path as string
   * @return list of all paths contained within the wildcard path
   */
  def globPath(path: String, fromEpochMillis: Option[Long] = None, untilEpochMillis: Option[Long] = None): Array[String] = {
    val conf = new Configuration()
    val fs = new Path(path).getFileSystem(conf)
    val paths = fs.globStatus(new Path(path))
    logger.log(Level.DEBUG, s"${path} expanded in ${paths.length} files")
    paths.map(wildString => {
      val path = wildString.getPath
      val pathString = path.toString
      val fileModEpochMillis = if (fromEpochMillis.nonEmpty) {
        Some(fs.listStatus(path).filter(_.isFile).head.getModificationTime)
      } else None
      (pathString, fileModEpochMillis)
    }).filter(p => {
      var switch = true
      if (p._2.nonEmpty) {
        if (fromEpochMillis.nonEmpty && fromEpochMillis.get < p._2.get) switch = false
        if (untilEpochMillis.nonEmpty && untilEpochMillis.get > p._2.get) switch = false
      }
      switch
    }).map(_._1)
  }

  /**
   * Return tables from a given database. Try to use Databricks' fast version if that fails for some reason, revert
   * back to using standard open source version
   * @param db
   * @return
   */
  // TODO: switch to the "SHOW TABLES" instead - it's much faster
  // TODO: also, should be a flag showing if we should omit temporary tables, etc.
  def getTables(db: String): Array[String] = {
    try {
      spark.sessionState.catalog.listTables(db).toDF.select(col("name")).as[String].collect()
    } catch {
      case _: Throwable => spark.catalog.listTables(db).rdd.map(row => row.name).collect()
    }
  }

  // TODO -- Simplify and combine the functionality of all three parOptimize functions below.

  /**
   * Parallel optimizer with support for vacuum and zordering. This version of parOptimize will optimize (and zorder)
   * all tables in a Database
   * @param db Database to optimize
   * @param parallelism How many tables to optimize at once. Be careful here -- if the parallelism is too high relative
   *                    to the cluster size issues will arise. There are also optimize parallelization configs to take
   *                    into account as well (i.e. spark.databricks.delta.optimize.maxThreads)
   * @param zOrdersByTable Map of tablename -> Array(field names) to be zordered. Order matters here
   * @param vacuum Whether or not to vacuum the tables
   * @param retentionHrs Number of hours for retention regarding vacuum. Defaulted to standard 168 hours (7 days) but
   *                     can be overridden. NOTE: the safeguard has been removed here, so if 0 hours is used, no error
   *                     will be thrown.
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
        val sql = s"""optimize ${db}.${tbl} ${zorderColumns}"""
        println(s"optimizing: ${db}.${tbl} --> $sql")
        spark.sql(sql)
        if (vacuum) {
          println(s"vacuuming: ${db}.${tbl}")
          spark.sql(s"vacuum ${db}.${tbl} RETAIN ${retentionHrs} HOURS")
        }
        println(s"Complete: ${db}.${tbl}")
      } catch {
        case e: Throwable => println(e.printStackTrace())
      }
    })
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
  }

  /**
   * Same purpose as parOptimize above but instead of optimizing an entire database, only specific tables are
   * optimized.
   * @param tables Array of Overwatch PipelineTable
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
        val sql = s"""optimize ${tbl.tableFullName} ${zorderColumns}"""
        println(s"optimizing: ${tbl.tableFullName} --> $sql")
        spark.sql(sql)
        if (tbl.vacuum > 0) {
          println(s"vacuuming: ${tbl.tableFullName}, Retention == ${tbl.vacuum}")
          spark.sql(s"VACUUM ${tbl.tableFullName} RETAIN ${tbl.vacuum} HOURS")
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
   * @param tables Fully-qualified database.tablename
   * @param parallelism Number of tables to optimize simultaneously
   */
  def parOptimizeTables(tables: Array[String],
                        parallelism: Int = parallelism - 1): Unit = {
    val tablesPar = tables.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    tablesPar.tasksupport = taskSupport

    tablesPar.foreach(tbl => {
      try {
        println(s"optimizing: ${tbl}")
        spark.sql(s"optimize ${tbl}")
        println(s"Complete: ${tbl}")
      } catch {
        case e: Throwable => println(e.printStackTrace())
      }
    })
  }

  // TODO -- Combine the following two functions

  /**
   * Manual compute stats function for an entire database -- useful for parquet tables but generally unecessary for delta tables.
   * Better to use the moveColumnsToFront and have those columns auto-indexed, in most cases.
   * TODO -- Add input param "computeDelta: Boolean" and if false, omit delta tables from being computed
   * @param db Database for which to compute stats Note: this will compute stats for all tables currently
   * @param parallelism How many tables to compute stats simultaneously
   * @param forColumnsByTable Which columns should have stats calculated.
   */
  def computeStats(db: String, parallelism: Int = parallelism - 1,
                   forColumnsByTable: Map[String, Array[String]] = Map()): Unit = {
    val tables = getTables(db)
    val tablesPar = tables.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    tablesPar.tasksupport = taskSupport

    tablesPar.foreach(tbl => {
      val forColumns = if (forColumnsByTable.contains(tbl)) s"for columns ${forColumnsByTable(tbl).mkString(", ")}" else ""
      val sql = s"""analyze table ${db}.${tbl} compute statistics ${forColumns}"""
      try {
        println(s"Analyzing: $tbl --> $sql")
        spark.sql(sql)
        println(s"Completed: $tbl")
      } catch {
        case e: Throwable => println(s"FAILED: $tbl --> $sql")
      }
    })
  }

  /**
   * Compute stats for Array of PipelineTables
   * @param tables
   */
  def computeStats(tables: Array[PipelineTable]): Unit = {
    val tablesPar = tables.par
    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    tablesPar.tasksupport = taskSupport

    tablesPar.foreach(tbl => {
      val forColumns = if (tbl.statsColumns.nonEmpty) s"for columns ${tbl.statsColumns.mkString(", ")}" else ""
      val sql = s"""analyze table ${tbl.tableFullName} compute statistics ${forColumns}"""
      try {
        println(s"Analyzing: ${tbl.tableFullName} --> $sql")
        spark.sql(sql)
        println(s"Completed: ${tbl.tableFullName}")
      } catch {
        case e: Throwable => println(s"FAILED: ${tbl.tableFullName} --> $sql")
      }
    })
  }

  /**
   * drop database cascade / drop table the standard functionality is serial. This function completes the deletion
   * of files in serial along with the call to the drop command. A faster way to do this is to call truncate and
   * then vacuum to 0 hours which allows for eventual consistency to take care of the cleanup in the background.
   * Be VERY CAREFUL with this function as it's a nuke. There's a different methodology to make this work depending
   * on the cloud platform. At present Azure and AWS are both supported
   * TODO - This function could be further improved by calling the fastrm function below, listing all files and dropping
   *  them in parallel, then dropping the table from the metastore. Testing needed to enable this.
   * @param fullTableName
   * @param cloudProvider
   */
  private[overwatch] def fastDrop(fullTableName: String, cloudProvider: String): Unit = {
    if (cloudProvider == "aws") {
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      spark.sql(s"truncate table ${fullTableName}")
      spark.sql(s"VACUUM ${fullTableName} RETAIN 0 HOURS")
      spark.sql(s"drop table if exists ${fullTableName}")
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    } else {
      Seq("").toDF("HOLD")
        .write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(fullTableName)
      spark.sql(s"drop table if exists ${fullTableName}")
    }
  }

  /**
   * Helper private function for fastrm. Enables serialization
   * This version only supports dbfs but s3 is easy to add it just wasn't necessary at the time this was written
   * TODO -- add support for s3/abfs direct paths
   * @param file
   */
  private def rmSer(file: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(new java.net.URI("dbfs:/"), conf)
    try {
      fs.delete(new Path(file), true)
    } catch {
      case e: Throwable => {
        logger.log(Level.ERROR, s"ERROR: Could not delete file $file, skipping", e)
      }
    }
  }

  /**
   * SERIALIZABLE drop function
   * Drop all files from an array of top-level paths in parallel. Top-level paths can have wildcards.
   * BE VERY CAREFUL with this function, it's a nuke.
   * @param topPaths Array of wildcard strings to act as parent paths. Every path that is returned from the glob of
   *                 globs will be dropped in parallel
   */
  private[overwatch] def fastrm(topPaths: Array[String]): Unit = {
    topPaths.map(p => {
      if (p.reverse.head.toString == "/") s"${p}*" else s"${p}/*"
    }).flatMap(p => globPath(p)).toSeq.toDF("filesToDelete")
      .as[String]
      .foreach(f => rmSer(f))

    topPaths.foreach(dir => dbutils.fs.rm(dir, true))

  }

}