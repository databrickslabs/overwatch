package com.databricks.labs.overwatch.utils

import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.core.io.JsonStringEncoder
import java.util.{Date, UUID}

import java.io.File
import org.apache.hadoop.fs.{FileSystem, Path, FileUtil}

import javax.crypto
import javax.crypto.KeyGenerator
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}


object JsonUtils {

  case class JsonStrings(prettyString: String, compactString: String, escapedString: String, fromObj: Any)

  private[overwatch] lazy val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  private val encoder = JsonStringEncoder.getInstance

  def jsonToMap(message: String): Map[String, Any] = {
    objectMapper.readValue(message, classOf[Map[String, Any]])
  }

  def objToJson(obj: Any): JsonStrings = {
    JsonStrings(
      objectMapper.writerWithDefaultPrettyPrinter.writeValueAsString(obj),
      objectMapper.writeValueAsString(obj),
      new String(encoder.quoteAsString(objectMapper.writeValueAsString(obj))),
      obj
    )
  }

}

class Cipher {

  private val tempKey = UUID.randomUUID().toString
  private val salt = Array[Byte](16)
  private val spec = new PBEKeySpec(tempKey.toCharArray, salt, 1000, 128 * 8)
  private val keyGenner = KeyGenerator.getInstance("AES")
  keyGenner.init(128)
  private val aesKey = keyGenner.generateKey()
  private val iv = new IvParameterSpec("0102030405060708".getBytes())
  private val cipher = crypto.Cipher.getInstance("AES/CBC/PKCS5Padding")

  private[overwatch] def encrypt(text: String): Array[Byte] = {
    //    val aesKey = new SecretKeySpec(tempKey.getBytes(), "AES")
    //    val aesKey = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1").generateSecret(spec)
    cipher.init(crypto.Cipher.ENCRYPT_MODE, aesKey, iv)
    cipher.doFinal(text.getBytes())
  }

  private[overwatch] def decrypt(byteStream: Array[Byte]): String = {
    cipher.init(crypto.Cipher.DECRYPT_MODE, aesKey, iv)
    new String(cipher.doFinal(byteStream))
  }


}

object SchemaTools {

  private def sanitizeFieldName(s: String): String = {
    s.replaceAll("[^a-zA-Z0-9_]", "")
  }

  private def sanitizeFields(field: StructField): StructField = {
    field.copy(name = sanitizeFieldName(field.name), dataType = sanitizeSchema(field.dataType))
  }

  private def generateUniques(fields: Array[StructField]): Array[StructField] = {
    val r = new scala.util.Random(10)
    val fieldNames = fields.map(_.name)
    val dups = fieldNames.diff(fieldNames.distinct)
    val dupCount = dups.length
    if (dupCount == 0) {
      fields
    } else {
      val uniqueSuffixes = (0 to dupCount + 10).map(_ => r.alphanumeric.take(6).mkString("")).distinct
      fields.zipWithIndex.map(f => {
        f._1.copy(name = f._1.name + "_" + uniqueSuffixes(f._2))
      })
    }
  }

  def sanitizeSchema(dataType: DataType): DataType = {
    dataType match {
      case dt: StructType =>
        val dtStruct = dt.asInstanceOf[StructType]
        dtStruct.copy(fields = generateUniques(dtStruct.fields).map(sanitizeFields))
      case dt: ArrayType =>
        val dtArray = dt.asInstanceOf[ArrayType]
        dtArray.copy(elementType = sanitizeSchema(dtArray.elementType))
      case _ => dataType
    }
  }
}

object Helpers extends SparkSessionWrapper {

  def SubtractTime(start: Column, end: Column): Column = {
    val runTimeMS = end - start
    val runTimeS = runTimeMS / 1000
    val runTimeM = runTimeS / 60
    val runTimeH = runTimeM / 60
    struct(
      start.alias("startEpochMS"),
      from_unixtime(start / 1000).alias("startTS"),
      end.alias("endEpochMS"),
      from_unixtime(end / 1000).alias("endTS"),
      lit(runTimeMS).alias("runTimeMS"),
      lit(runTimeS).alias("runTimeS"),
      lit(runTimeM).alias("runTimeM"),
      lit(runTimeH).alias("runTimeH")
    ).alias("RunTime")
  }

  def pathExists(path: String): Boolean = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.exists(new Path(path))
  }

  def getFullPath(path:String): String = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.getFileStatus(new Path(path)).getPath.toString
  }

  def getAllFiles(path:String): Seq[String] = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val files = fs.listStatus(new Path(path))
    files.map(_.getPath.toString)
  }

  def globPath(path: String): Array[String] = {
    val conf = sc.hadoopConfiguration
    val glob = new Path(path)
    val fileSystem = FileSystem.get(conf)
    val allStatus = fileSystem.globStatus(glob)
    val allPath = FileUtil.stat2Paths(allStatus)
    allPath.map(_.toString)
  }

}