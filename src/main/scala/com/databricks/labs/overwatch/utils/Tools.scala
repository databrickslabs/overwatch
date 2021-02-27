package com.databricks.labs.overwatch.utils

import com.amazonaws.services.s3.model.AmazonS3Exception
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.labs.overwatch.pipeline.PipelineTable
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.io.JsonStringEncoder
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import javax.crypto
import javax.crypto.KeyGenerator
import javax.crypto.spec.IvParameterSpec
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

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
      obj.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false)
    }
    if (!includeEmpty) {
      obj.setSerializationInclusion(Include.NON_EMPTY)
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
      val cleanMessage = StringEscapeUtils.unescapeJson(message)
      defaultObjectMapper.readValue(cleanMessage, classOf[Map[String, Any]])
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
 * This entire class was created in an attempt to ensure all API keys remain encrypted between module and that
 * no keys were ever logged. This was abandoned due to some errors but should be revisited. This code should either
 * be removed (along with removed from the APIEnv structure) after it's confirmed that the API key is not stored
 * anywhere in clear text in logs or otherwise.
 *
 * @param key
 */
class Cipher(key: String) {

  private val salt = Array[Byte](16)
  private val keyGenner = {
    val k = KeyGenerator.getInstance("AES")
    k.init(128)
    k
  }
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

  def flattenSchema(df: DataFrame): Array[Column] = {
    flattenSchema(df.schema)
  }

  /**
   *
   * @param schema
   * @param prefix
   * @return
   */
  def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, columnName)
        case _ => Array(col(columnName).as(columnName.replace(".", "_")))
      }
    })
  }

  /**
   *
   * TODO: should it handle things like Array/Map(Struct) ?
   *
   * @param schema
   * @param prefix
   * @return
   */
  def getAllColumnNames(schema: StructType, prefix: String = null): Array[String] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => getAllColumnNames(st, columnName)
        case _ => Array(columnName)
      }
    })
  }

  def modifyStruct(structToModify: StructType, changeInventory: Map[String, Column], prefix: String = null): Array[Column] = {
    structToModify.fields.map(f => {
      val fullFieldName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case c: StructType => {
          changeInventory.getOrElse(fullFieldName, struct(modifyStruct(c, changeInventory, prefix = fullFieldName): _*)).alias(f.name)
        }
        case _ => changeInventory.getOrElse(fullFieldName, col(fullFieldName)).alias(f.name)
      }
    })
  }

  def nestedColExists(df: DataFrame, dotPathOfField: String): Boolean = {
    val dotPathAr = dotPathOfField.split("\\.")
    val elder = dotPathAr.head
    val children = dotPathAr.tail
    val startField = df.schema.fields.find(_.name == elder)
    try {
      children.foldLeft(startField) {
        case (f, childName) => {
          if (f.nonEmpty) {
            f.get.dataType match {
              case child: StructType => child.find(_.name == childName)
            }
          } else None
        }
      }.nonEmpty
    } catch {
      case e: Throwable => {
        logger.log(Level.WARN, s"${children.takeRight(1).head} column not found in source DF, attempting to continue without it", e)
        false
      }
    }
    //    jobsSnapshot.asDF().schema.fields.filter(_.name == "settings").head.dataType.asInstanceOf[StructType].fields.filter(_.name == "existing_cluster_id").map(_.name).headOption
  }

  //  def modifyStruct(df: DataFrame, structColToModify: String, changeInventory: Map[String, Column], prefix: String = null): Column = {
  //    //    val columnInventory = df.select(s"${structColToModify}.*").columns
  //    //    val newStructInventory = columnInventory.map(cName => )
  //    val columnInventory = getAllColumnNames(df.select(s"${structColToModify}.*").schema).map(cn => {
  //      if (cn.contains(".")) {
  //        cn.split("\\.").dropRight(1).mkString(".")
  //      } else { cn }
  //    }).distinct
  //    // returning flat columns, need to return the struct column and stop
  //    val newStructInventory = columnInventory.map(cName => {
  //      changeInventory.getOrElse(cName, col(s"${structColToModify}.${cName}")).alias(cName)
  //    })
  //    struct(newStructInventory: _*)
  //  }

  // TODO -- How to remove keys with nulls from maps?
  def structToMap(df: DataFrame, colToConvert: String, dropEmptyKeys: Boolean = true): Column = {

    val mapColName = colToConvert.split("\\.").reverse.head
    val removeEmptyKeys = udf((m: Map[String, String]) => m.filterNot(_._2 == null))

    val dfFlatColumnNames = getAllColumnNames(df.schema)
    if (dfFlatColumnNames.exists(_.startsWith(colToConvert))) {
      val schema = df.select(s"${colToConvert}.*").schema
      val mapCols = collection.mutable.LinkedHashSet[Column]()
      schema.fields.foreach(field => {
        mapCols.add(lit(field.name))
        mapCols.add(col(s"${colToConvert}.${field.name}").cast("string"))
      })
      val newRawMap = map(mapCols.toSeq: _*)
      if (dropEmptyKeys) {
        when(newRawMap.isNull, lit(null)).otherwise(removeEmptyKeys(newRawMap)).alias(mapColName)
      } else newRawMap.alias(mapColName)
    } else {
      lit(null).cast(MapType(StringType, StringType, true)).alias(mapColName)
    }
  }

  //    .cast(MapType(StringType, StringType, true))

  // TODO -- Delta writer is schema case sensitive and will fail on write if column case is not identical on both sides
  //  As such, schema case sensitive validation needs to be enabled and a handler for whether to assume the same data
  //  and merge the data, or drop it, or quarantine it or what. This is very common in cases where a column is of
  //  struct type but the key's are derived via user-input (i.e. event log "properties" field).
  // TODO -- throw exception if the resulting string is empty
  /**
   * Remove special characters from the field name
   *
   * @param s
   * @return
   */
  private def sanitizeFieldName(s: String): String = {
    s.replaceAll("[^a-zA-Z0-9_]", "")
  }

  /**
   * Clean field name and recurse
   *
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
   *
   * @param fields
   * @return
   */
  private def generateUniques(fields: Array[StructField]): Array[StructField] = {
    val caseSensitive = spark.conf.get("spark.sql.caseSensitive").toBoolean
    val r = new scala.util.Random(42L) // Using seed to reuse suffixes on continuous duplicates
    val fieldNames = if(caseSensitive) {
      fields.map(_.name.trim)
    } else fields.map(_.name.trim.toLowerCase())
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
        val fieldName = if (caseSensitive) f._1.name else f._1.name.toLowerCase
        if (dups.contains(fieldName)) {
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
   *
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
   * TODO -- convert to same pattern as schema validation function
   *
   * @param df Input dataframe to be cleansed
   * @return
   */
  def scrubSchema(df: DataFrame): DataFrame = {
    spark.createDataFrame(df.rdd, SchemaTools.sanitizeSchema(df.schema).asInstanceOf[StructType])
  }

  /**
   * The next several set of functions are used to clean a schema with a minimum required set of fields as the guide.
   * Rules will be documented later but this section will likely benefit from a refactor to a more complex object
   */
    // TODO -- Review viability of similifying all schema functions within the SchemaHelpers object
    //  during next refactor


  private def getPrefixedString(prefix: Option[String], fieldName: String): String = {
    if (prefix.isEmpty) fieldName else s"${prefix.get}.${fieldName}"
  }

  private def isComplexDataType(dataType: DataType): Boolean = {
    dataType.isInstanceOf[ArrayType] || dataType.isInstanceOf[StructType] || dataType.isInstanceOf[MapType]
  }

  private def emitExclusionWarning(f: StructField, isDebug: Boolean, prefix: Option[String]): Unit = {
    val fullColName = getPrefixedString(prefix, f.name)

    val msg = s"SCHEMA WARNING: COLUMN STRIPPED from source --> Column $fullColName is required to be absent from " +
      s"source as it's type has been identified as a NullType"
    logger.log(Level.DEBUG, msg)
  }

  private def malformedSchemaErrorMessage(f: StructField, req: StructField, prefix: Option[String]): String = {
    val fullColName = getPrefixedString(prefix, f.name)
    val requiredTypeName = req.dataType.typeName
    val fieldTypeName = f.dataType.typeName

    val oneComplexOneNot = (isComplexDataType(req.dataType) && !isComplexDataType(f.dataType)) ||
        (!isComplexDataType(req.dataType) && isComplexDataType(f.dataType))

    val bothComplex = isComplexDataType(req.dataType) || isComplexDataType(f.dataType)

    val neitherComplex = !isComplexDataType(req.dataType) && !isComplexDataType(f.dataType)

    val genericSchemaErrorMsg = s"SCHEMA ERROR: Received type $fieldTypeName for $fullColName BUT required type " +
      s"is $requiredTypeName"
    val unsupportedComplexCastingMsg = s"SCHEMA ERROR: Required Schema for column $fullColName is $requiredTypeName " +
      s"but input type was $fieldTypeName. Implicit casting between / to / from complex types not supported."

    // if one field is complex and the other is not, fail
    //noinspection TypeCheckCanBeMatch
    if (oneComplexOneNot) unsupportedComplexCastingMsg
    else if (req.dataType.isInstanceOf[ArrayType] && requiredTypeName == fieldTypeName) {
      // Both Array but Element Type not Equal
      val reqElementTypeName = req.dataType.asInstanceOf[ArrayType].elementType.typeName
      val fieldElementTypeName = f.dataType.asInstanceOf[ArrayType].elementType.typeName
      if (reqElementTypeName != fieldElementTypeName) {
        s"SCHEMA ERROR: Array element types incompatible: Received array<$fieldElementTypeName> when " +
          s"array<$reqElementTypeName> is required for column $fullColName. Implicit casting of Array " +
          s"element types is not supported."
      } else unsupportedComplexCastingMsg // cannot cast array types
    }
    else if (bothComplex && req.dataType != f.dataType) unsupportedComplexCastingMsg // complex type casting not supported
    else if (neitherComplex) { // both simple types -- will attempt to cast
      val scalarCastMessage = s"SCHEMA WARNING: IMPLICIT CAST: Required Type for column: " +
        s"$fullColName is $requiredTypeName but received $fieldTypeName. " +
        s"Attempting to cast to required type but may result in unexpected nulls or loss of precision"
      logger.log(Level.DEBUG, scalarCastMessage)
      scalarCastMessage
    } else genericSchemaErrorMsg
  }

  private def checkNullable(missingField: StructField, prefix: Option[String], isDebug: Boolean): Unit = {
    val fullColName = getPrefixedString(prefix, missingField.name)

    if (missingField.nullable) { // if nulls allowed
      val msg = s"SCHEMA WARNING: Input DF missing required field $fullColName of type " +
        s"${missingField.dataType.typeName}. There's either an error or relevant data doesn't exist in " +
        s"your environment and/or was not acquired during the current run."
      if (isDebug) println(msg)
      logger.log(Level.DEBUG, msg)
    } else { // FAIL --> trying to null non-nullable field
      val msg = s"SCHEMA ERROR: Required Field $fullColName is NON-NULLABLE but nulls were received. Failing module"
      if (isDebug) println(msg)
      logger.log(Level.ERROR, msg)
      throw new BadSchemaException(msg)
    }
  }

  private def malformedStructureHandler(f: StructField, req: StructField, prefix: Option[String]): Exception = {
    val msg = malformedSchemaErrorMessage(f, req, prefix)
    println(msg)
    logger.log(Level.ERROR, msg)
    new BadSchemaException(msg)
  }

  def buildValidationRunner(
                                     dfSchema: StructType,
                                     minRequiredSchema: StructType,
                                     enforceNonNullCols: Boolean = true,
                                     isDebug: Boolean = false,
                                     cPrefix: Option[String] = None
                                   ): Seq[ValidatedColumn] = {

    // Some fields are force excluded from the complex types due to duplicates or issues in underlying
    // data sources
    val exclusionsFields = minRequiredSchema.fields.filter(f => f.dataType.isInstanceOf[NullType])
    val exclusions = exclusionsFields.map(_.name.toLowerCase)
    exclusionsFields.foreach(emitExclusionWarning(_, isDebug, cPrefix))

    val dfFieldsNames = dfSchema.fieldNames.map(_.toLowerCase)
    val reqFieldsNames = minRequiredSchema.fieldNames.map(_.toLowerCase)
    val missingRequiredFieldNames = reqFieldsNames.diff(dfFieldsNames).diff(exclusions)
    val unconstrainedFieldNames = dfFieldsNames.diff(reqFieldsNames).diff(exclusions)
    val fieldsNamesRequiringValidation = dfFieldsNames.intersect(reqFieldsNames).diff(exclusions)

    // find missing required fields, add, type, and alias them
    val missingFields = minRequiredSchema.filter(child => missingRequiredFieldNames.contains(child.name.toLowerCase))
      .map(missingChild => {
        if (enforceNonNullCols) checkNullable(missingChild, cPrefix, isDebug)
        ValidatedColumn(lit(null).cast(missingChild.dataType).alias(missingChild.name))
      })

    // fields without requirements
    // test field and recurse at lower layer
    val unconstrainedFields = dfSchema.filter(f => unconstrainedFieldNames.contains(f.name.toLowerCase))
      .map(f => {
        ValidatedColumn(col(getPrefixedString(cPrefix, f.name)))
      })

    // fields to validate
    val fieldsToValidate = dfSchema.filter(f => fieldsNamesRequiringValidation.contains(f.name.toLowerCase))
      .map(f => {
        ValidatedColumn(
          col(getPrefixedString(cPrefix, f.name)),
          dfSchema.fields.find(_.name.toLowerCase == f.name.toLowerCase),
          minRequiredSchema.fields.find(_.name.toLowerCase == f.name.toLowerCase)
        )
      })

    missingFields ++ unconstrainedFields ++ fieldsToValidate

  }

  def validateSchema(
                      validator: ValidatedColumn,
                      cPrefix: Option[String] = None,
                      enforceNonNullCols: Boolean = true,
                      isDebug: Boolean = false
                    ): ValidatedColumn = {

    if (validator.requiredStructure.nonEmpty) { // is requirement on the field
      val fieldStructure = validator.fieldToValidate.get
      val requiredFieldStructure = validator.requiredStructure.get
      val newPrefix = Some(getPrefixedString(cPrefix, fieldStructure.name))

      fieldStructure.dataType match {
        case dt: StructType => { // field is struct type
          val dtStruct = dt.asInstanceOf[StructType]
          if (!requiredFieldStructure.dataType.isInstanceOf[StructType]) // requirement is not struct field
            throw malformedStructureHandler(fieldStructure, requiredFieldStructure, cPrefix)

          // When struct type matches identify requirements and recurse
          val validatedChildren = buildValidationRunner(
            dtStruct,
            requiredFieldStructure.dataType.asInstanceOf[StructType],
            enforceNonNullCols,
            isDebug,
            newPrefix
          )
            .map(validateSchema(_, newPrefix)) // TODO -- check if recursive prefix is right

          // build and return struct
          validator.copy(column = struct(validatedChildren.map(_.column): _*).alias(fieldStructure.name))

        }
        case dt: ArrayType => { // field is ArrayType
          val dtArray = dt.asInstanceOf[ArrayType]
          // field is array BUT requirement is NOT array --> throw error
          if (!requiredFieldStructure.dataType.isInstanceOf[ArrayType])
            throw malformedStructureHandler(fieldStructure, requiredFieldStructure, cPrefix)

          // array[complexType]
          // if elementType is nested complex, recurse to validate and return array of validated children
          dtArray.elementType match {
            case eType: StructType =>
              val validatedChildren = buildValidationRunner(
                eType,
                requiredFieldStructure.dataType.asInstanceOf[StructType],
                enforceNonNullCols,
                isDebug,
                newPrefix
              ).map(validateSchema(_, newPrefix))

              // build and return array(struct)
              validator.copy(column = array(struct(validatedChildren.map(_.column): _*)).alias(fieldStructure.name))

            case eType =>
              if (eType != requiredFieldStructure.dataType.asInstanceOf[ArrayType].elementType) { //element types don't match FAIL
                throw malformedStructureHandler(fieldStructure, requiredFieldStructure, cPrefix)
              } else { // element types match
                validator.copy(column = col(getPrefixedString(cPrefix, fieldStructure.name)).alias(fieldStructure.name))
              }
          }
        }
        // TODO -- add support for MapType
        case scalarField => { // field is base scalar
          if (isComplexDataType(requiredFieldStructure.dataType)) { // FAIL -- Scalar to Complex not supported
            throw malformedStructureHandler(fieldStructure, requiredFieldStructure, cPrefix)
          }
          if (scalarField != requiredFieldStructure.dataType) { // if not same type, try to cast column to the required type;
            val warnMsg = malformedSchemaErrorMessage(fieldStructure, requiredFieldStructure, cPrefix)
            logger.log(Level.WARN, warnMsg)
            if (isDebug) println(warnMsg)
            val mutatedColumnt = col(getPrefixedString(cPrefix, fieldStructure.name))
              .cast(requiredFieldStructure.dataType).alias(fieldStructure.name)
            validator.copy(column = mutatedColumnt)
          } else { // colType is simple and passes requirements
            validator.copy(column = col(getPrefixedString(cPrefix, fieldStructure.name)).alias(fieldStructure.name))
          }
        }
      }
    } else { // no validation required
      validator
    }
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
   *
   * @param path
   * @return
   */
  def pathExists(path: String): Boolean = {
    val conf = new Configuration()
    val fs = new Path(path).getFileSystem(conf)
    fs.exists(new Path(path))
  }

  /**
   * Serializable path expander from wildcard paths. Given an input like /path/to/<asterisk>/wildcards/<asterisk>
   * all paths in that wildcard path will be returned in the array. The key to the performance of this function
   * is ensuring spark is used to serialize it meaning make sure that it's called from the lambda of a Dataset
   *
   * TODO - This function can be easily enhanced to take in String* so that multiple, unrelated wildcards can be
   * globbed simultaneously
   *
   * @param path wildcard path as string
   * @return list of all paths contained within the wildcard path
   */
  def globPath(path: String, fromEpochMillis: Option[Long] = None, untilEpochMillis: Option[Long] = None): Array[(String, Option[Long], Boolean)] = {
    val conf = new Configuration()
    try {
      val fs = new Path(path).getFileSystem(conf)
      val paths = fs.globStatus(new Path(path))
      logger.log(Level.DEBUG, s"$path expanded in ${paths.length} files")
      paths.map(wildString => {
        val path = wildString.getPath
        val pathString = path.toString
        val fileModEpochMillis = if (fromEpochMillis.nonEmpty) {
          // TODO -- use the listStatus to return other metadata as well such as file size
          Some(fs.listStatus(path).filter(_.isFile).head.getModificationTime)
        } else None
        (pathString, fileModEpochMillis)
      }).filter(p => {
        // p._1 == path
        // p._2 == fileModEpochMillis
        // Ensure that the last modified time of the file was between from --> until
        if (p._2.nonEmpty) {
          val lastModifiedTS = p._2.get
          // TODO -- Switch to Debug -- is on exec logs with println
          println(s"PROOF: ${p._1} --> ${fromEpochMillis.getOrElse(0)} <= ${lastModifiedTS} < ${untilEpochMillis.getOrElse(0)}")
          (fromEpochMillis.nonEmpty && fromEpochMillis.get <= lastModifiedTS) &&
            untilEpochMillis.nonEmpty && untilEpochMillis.get > lastModifiedTS
        } else false
      }).map(x => (x._1, x._2, true))
    } catch {
      case e: AmazonS3Exception => logger.log(Level.ERROR, s"ACCESS DENIED: " +
        s"Cluster Event Logs at path ${path} are inaccessible with given the Databricks account used to run Overwatch. " +
        s"Validate access & try again.\n${e.getMessage}")
        Array((path, Some(0L), false))
    }
  }

  /**
   * Return tables from a given database. Try to use Databricks' fast version if that fails for some reason, revert
   * back to using standard open source version
   *
   * @param db
   * @return
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
   *
   * @param db                Database for which to compute stats Note: this will compute stats for all tables currently
   * @param parallelism       How many tables to compute stats simultaneously
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
   *
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
   * them in parallel, then dropping the table from the metastore. Testing needed to enable this.
   *
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
   *
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
   *
   * @param topPaths Array of wildcard strings to act as parent paths. Every path that is returned from the glob of
   *                 globs will be dropped in parallel
   */
  private[overwatch] def fastrm(topPaths: Array[String]): Unit = {
    topPaths.map(p => {
      if (p.reverse.head.toString == "/") s"${p}*" else s"${p}/*"
    }).flatMap(p => globPath(p)).toSeq.toDF("filesToDelete", "fileCreateDate", "succeeded")
      .select('filesToDelete).as[String]
      .foreach(f => rmSer(f))

    topPaths.foreach(dir => dbutils.fs.rm(dir, true))

  }

}
