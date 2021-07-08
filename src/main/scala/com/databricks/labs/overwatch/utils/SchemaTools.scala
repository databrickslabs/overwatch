package com.databricks.labs.overwatch.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Random

/**
 * SchemaTools is one of the more complex objects in Overwatch as it handles the schema (or lack there of rather)
 * evolution, oddities, and edge cases found when working with the event / audit logs. As of the 0.2 version, there's
 * still significant room for improvement here but it seems to be handling the challenges for now.
 *
 * Spark in general ignored column name case, but delta does not. Delta will throw an error if a dataframe has two
 * columns with the same name but where the name has a different case. This is not well-handled here and should be
 * added.
 */
object SchemaTools extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def getSchemaVersion(dbName: String): String = {
    val dbMeta = spark.sessionState.catalog.getDatabaseMetadata(dbName)
    dbMeta.properties.getOrElse("SCHEMA", "UNKNOWN")
  }

  def modifySchemaVersion(dbName: String, targetVersion: String): Unit = {
    val existingSchemaVersion = SchemaTools.getSchemaVersion(dbName)
    val upgradeStatement =
      s"""ALTER DATABASE $dbName SET DBPROPERTIES
         |(SCHEMA=$targetVersion)""".stripMargin

    val upgradeMsg = s"upgrading schema from $existingSchemaVersion --> $targetVersion with STATEMENT:\n " +
      s"$upgradeStatement"
    logger.log(Level.INFO, upgradeMsg)
    spark.sql(upgradeStatement)
    val newSchemaVersion = SchemaTools.getSchemaVersion(dbName)
    assert(newSchemaVersion == targetVersion)
    logger.log(Level.INFO, s"UPGRADE SUCCEEDED")

  }

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
        case c: StructType =>
          changeInventory.getOrElse(fullFieldName, struct(modifyStruct(c, changeInventory, prefix = fullFieldName): _*)).alias(f.name)
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
        case (f, childName) =>
          if (f.nonEmpty) {
            f.get.dataType match {
              case child: StructType => child.find(_.name == childName)
            }
          } else None
      }.nonEmpty
    } catch {
      case e: Throwable =>
        logger.log(Level.WARN, s"${children.takeRight(1).head} column not found in source DF, attempting to continue without it", e)
        false
    }
  }

  // TODO -- Remove keys with nulls from maps?
  //  Add test to ensure that null/"" key and null/"" value are both handled
  //  as of 0.4.1 failed with key "" in spark_conf
  //  TEST for multiple null/"" cols / keynames in same struct/record
  def structToMap(df: DataFrame, colToConvert: String, dropEmptyKeys: Boolean = true): Column = {

    val mapColName = colToConvert.split("\\.").reverse.head
    val removeEmptyKeys = udf((m: Map[String, String]) => m.filterNot(_._2 == null))

    val dfFlatColumnNames = getAllColumnNames(df.schema)
    if (dfFlatColumnNames.exists(_.startsWith(colToConvert))) {
      val schema = df.select(s"${colToConvert}.*").schema
      val mapCols = collection.mutable.LinkedHashSet[Column]()
      schema.fields.foreach(field => {
        val kRaw = field.name.trim
        val k = if (kRaw.isEmpty || kRaw == "") {
          val errMsg = s"SCHEMA WARNING: Column $colToConvert is being converted to a map but has a null key value. " +
            s"This key value will be replaced with a 'null_<random_string>' but should be corrected."
          logger.log(Level.WARN, errMsg)
          println(errMsg)
          s"null_${randomString(Some(42L), 6)}"
        } else kRaw
        mapCols.add(lit(k))
        mapCols.add(col(s"${colToConvert}.${field.name}").cast("string"))
      })
      val newRawMap = map(mapCols.toSeq: _*)
      if (dropEmptyKeys) {
        when(newRawMap.isNull, lit(null)).otherwise(removeEmptyKeys(newRawMap)).alias(mapColName)
      } else newRawMap.alias(mapColName)
    } else {
      lit(null).cast(MapType(StringType, StringType, valueContainsNull = true)).alias(mapColName)
    }
  }

  def randomString(seed: Option[Long] = None, length: Int = 10): String = {
    val r = if (seed.isEmpty) new Random() else new Random(seed.get) // Using seed to reuse suffixes on continuous duplicates
    r.alphanumeric.take(length).mkString("")
  }

  def uniqueRandomStrings(uniquesNeeded: Option[Int] = None, seed: Option[Long] = None, length: Int = 10): Seq[String] = {
    (0 to uniquesNeeded.getOrElse(500) + 10).map(_ => randomString(seed, length)).distinct
  }

  // TODO -- Delta writer is schema case sensitive and will fail on write if column case is not identical on both sides
  //  As such, schema case sensitive validation needs to be enabled and a handler for whether to assume the same data
  //  and merge the data, or drop it, or quarantine it or what. This is very common in cases where a column is of
  //  struct type but the key's are derived via user-input (i.e. event log "properties" field).
  //  UPDATE: This has been handled with spark.conf.get("spark.sql.caseSensitive") but needs to be tested on structs
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
    //    val r = new scala.util.Random(42L) // Using seed to reuse suffixes on continuous duplicates
    val fieldNames = if (caseSensitive) {
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
      val uniqueSuffixes = uniqueRandomStrings(Some(fields.length), None, 6)
      fields.zipWithIndex.map(f => {
        val fieldName = if (caseSensitive) f._1.name else f._1.name.toLowerCase
        if (dups.contains(fieldName)) {
          val generatedUniqueName = f._1.name + "_UNIQUESUFFIX_" + uniqueSuffixes(f._2)
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
   * Issue_86
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
   * Issue_86
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

  private def emitExclusionWarning(f: StructField, prefix: Option[String]): Unit = {
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
    exclusionsFields.foreach(emitExclusionWarning(_, cPrefix))

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
        case dt: StructType => // field is struct type
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
            .map(validateSchema(_, newPrefix))

          // build and return struct
          validator.copy(column = struct(validatedChildren.map(_.column): _*).alias(fieldStructure.name))
        case dt: ArrayType => // field is ArrayType
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
                val warnMsg = malformedSchemaErrorMessage(fieldStructure, requiredFieldStructure, cPrefix)
                logger.log(Level.WARN, warnMsg)
                if (isDebug) println(warnMsg)
                val mutatedColumn = col(getPrefixedString(cPrefix, fieldStructure.name))
                  .cast(requiredFieldStructure.dataType).alias(fieldStructure.name)
                validator.copy(column = mutatedColumn)
              } else { // element types match
                validator.copy(column = col(getPrefixedString(cPrefix, fieldStructure.name)).alias(fieldStructure.name))
              }
          }
        // TODO -- Issue_86 -- add support for MapType
        case scalarField => // field is base scalar
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
    } else { // no validation required
      validator
    }
  }
}

