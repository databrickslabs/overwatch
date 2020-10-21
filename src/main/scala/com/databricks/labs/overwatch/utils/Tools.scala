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

  def objToJson(obj: Any, includeNulls: Boolean = false, includeEmpty: Boolean = false): JsonStrings = {
    if (!includeNulls) objectMapper.setSerializationInclusion(Include.NON_NULL)
    if (!includeEmpty) objectMapper.setSerializationInclusion(Include.NON_EMPTY)
    JsonStrings(
      objectMapper.writerWithDefaultPrettyPrinter.writeValueAsString(obj),
      objectMapper.writeValueAsString(obj),
      new String(encoder.quoteAsString(objectMapper.writeValueAsString(obj))),
      obj
    )
  }

}

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
  private def sanitizeFieldName(s: String): String = {
    s.replaceAll("[^a-zA-Z0-9_]", "")
  }

  private def sanitizeFields(field: StructField): StructField = {
    field.copy(name = sanitizeFieldName(field.name), dataType = sanitizeSchema(field.dataType))
  }

  private def generateUniques(fields: Array[StructField]): Array[StructField] = {
    val r = new scala.util.Random(42L) // Using seed to reuse suffixes on continuous duplicates
    val fieldNames = fields.map(_.name.trim.toLowerCase())
    val dups = fieldNames.diff(fieldNames.distinct)
    val dupCount = dups.length
    if (dupCount == 0) {
      fields
    } else {
      logger.log(Level.WARN, s"Schema Found with duplicate field names. Correcting for this run but data " +
        s"should be merged and repaired.\nFIELDS: ${dups.mkString(", ")}")
      val uniqueSuffixes = (0 to fields.length + 10).map(_ => r.alphanumeric.take(6).mkString("")).distinct.map(_.toLowerCase.trim)
      fields.zipWithIndex.map(f => {
        if (dups.contains(f._1.name.trim.toLowerCase())) {
          val uniqueFieldName = f._1.name + "_" + uniqueSuffixes(f._2)
          logger.log(Level.WARN, s"FIELDS RENAMED: \n ${f._1.name} --> ${uniqueFieldName}")
          f._1.copy(name = uniqueFieldName)
        }
        else f._1
      })
    }
  }

  def sanitizeSchema(dataType: DataType): DataType = {
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

  def scrubSchema(df: DataFrame): DataFrame = {
    spark.createDataFrame(df.rdd, SchemaTools.sanitizeSchema(df.schema).asInstanceOf[StructType])
  }

  def moveColumnsToFront(df: DataFrame, colsToMove: Array[String]): DataFrame = {
    val dropSuffix = UUID.randomUUID().toString.replace("-", "")
    colsToMove.foldLeft(df) {
      case (df, c) =>
        val tempColName = s"${c}_${dropSuffix}"
        df.selectExpr(s"$c as $tempColName", "*").drop(c).withColumnRenamed(tempColName, c)
    }
  }

}

object Helpers extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)
  private val driverCores = java.lang.Runtime.getRuntime.availableProcessors()

  import spark.implicits._

  private def parallelism: Int = {
    Math.min(driverCores, 8)
  }


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
  //  def getLocalTime(ts: Column, tz: String): Column = {
  //    ts.expr.dataType match {
  //      case _: TimestampType => from_utc_timestamp(ts, tz)
  //      case _: LongType => from_utc_timestamp(from_unixtime(ts.cast("double") / 1000).cast("timestamp"), tz)
  //      case _: DoubleType => from_utc_timestamp(from_unixtime(ts).cast("timestamp"), tz)
  //      case _: IntegerType => from_utc_timestamp(from_unixtime(ts).cast("timestamp"), tz)
  //    }
  //  }

  def pathExists(path: String): Boolean = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.exists(new Path(path))
  }

  def getFullPath(path: String): String = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.getFileStatus(new Path(path)).getPath.toString
  }

  def getAllFiles(path: String): Seq[String] = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val files = fs.listStatus(new Path(path))
    files.map(_.getPath.toString)
  }

  // TODO - change this to the faster glob path here
  //  https://databricks.slack.com/archives/G95GCH8LT/p1589320667122200?thread_ts=1589317810.117200&cid=G95GCH8LT
  //  def globPath(path: String): Array[String] = {
  //    val hadoopConf = spark.sessionState.newHadoopConf()
  //    val driverFS = new Path(path).getFileSystem(hadoopConf)
  //    val paths = driverFS.globStatus(new Path(path))
  //    // TODO -- Switch this to DEBUG
  //    logger.log(Level.INFO, s"${path} expanded in ${paths.length} files")
  //    paths.map(_.getPath.toString)
  //  }

  def globPath(path: String): Array[String] = {
    val conf = new Configuration()
    val fs = new Path(path).getFileSystem(conf)
    val paths = fs.globStatus(new Path(path))
    // TODO -- Switch this to DEBUG
    logger.log(Level.INFO, s"${path} expanded in ${paths.length} files")
    paths.map(_.getPath.toString)
  }

  def getTables(db: String): Array[String] = {
    spark.catalog.listTables(db).rdd.map(row => row.name).collect()
  }

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

  private[overwatch] def fastrm(topPaths: Array[String]): Unit = {
    topPaths.map(p => {
      if (p.reverse.head.toString == "/") s"${p}*" else s"${p}/*"
    }).flatMap(globPath).toSeq.toDF("filesToDelete")
      .as[String]
      .foreach(f => rmSer(f))

    topPaths.foreach(dir => dbutils.fs.rm(dir, true))

  }

}