package com.databricks.labs.overwatch.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

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
   * Returns a StructField from a df by fieldName.
   * Case sensitivity is determined by spark.sql.caseSensitive
   * Currently not recursive
   * TODO - make this recursive with dot delimiter
   * @param df
   * @param colName
   * @return
   */
  def colByName(df: DataFrame)(colName: String): StructField = {
    if (spark.conf.get("spark.sql.caseSensitive") == "true") {
      df.schema.find(_.name == colName).get
    } else {
      df.schema.find(_.name.toLowerCase() == colName.toLowerCase()).get
    }
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

  /**
   * Removes nested columns within a struct and returns the Dataframe with the struct less the columns
   * @param df df containing struct with cols to remove
   * @param structToModify name of struct field to modify -- currently must be a top level field
   *                       recursion is not yet enabled
   * @param nestedFieldsToCull fields to remove -- top leve fields only, recursion not yet enabled
   * @return
   */
  def cullNestedColumns(df: DataFrame, structToModify: String, nestedFieldsToCull: Array[String]): DataFrame = {
    val originalFieldNames = df.select(s"$structToModify.*").columns
    val remainingFieldNames = if (spark.conf.get("spark.sql.caseSensitive") == "true") {
      originalFieldNames.diff(nestedFieldsToCull)
    } else {
      originalFieldNames.filterNot(f => nestedFieldsToCull.map(_.toLowerCase).contains(f.toLowerCase))
    }
    val newStructCol = struct(remainingFieldNames.map(f => s"$structToModify.$f") map col: _*).alias(structToModify)
    df.withColumn(structToModify, newStructCol)
  }

  /**
   * recurse through a struct and apply a transformation to all fields in the structed as noted in the
   * changeInventory. Enabled for nested fields, they can be accessed through dot notation such as
   * parent.child.grandchild
   * @param structToModify which struct to modify, recursion supported through dot notation
   * @param changeInventory Inventory of all changes to be made where the key is the dot map to the struct field
   *                        to modify and the column is the columnar expression of the transform
   * @param prefix initializes to null and appends as recursion drills down into the schema
   * @return
   */
  def modifyStruct(structToModify: StructType, changeInventory: Map[String, Column], prefix: String = null): Array[Column] = {
    structToModify.fields.map(f => {
      val fullFieldName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case c: StructType =>
          changeInventory.getOrElse(fullFieldName, struct(modifyStruct(c, changeInventory, prefix = fullFieldName): _*).alias(f.name))
        case _ => changeInventory.getOrElse(fullFieldName, col(fullFieldName)).alias(f.name)
      }
    })
  }

  /**
   * Identifies whether a field exists in a struct. This supports recursion through dot map notation.
   * parent.child.grandchild
   * if parent.child.grandchild exists in the schema func will return true
   * @param schema schema to analyze
   * @param dotPathOfField dot map notation parent.child.grandchild to check field
   * @return
   */
  def nestedColExists(schema: StructType, dotPathOfField: String): Boolean = {
    val dotPathAr = dotPathOfField.split("\\.")
    val elder = dotPathAr.head
    val children = dotPathAr.tail
    val startField = schema.fields.find(_.name == elder)
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

  /**
   * Converts a string column containing valid JSON to a struct field
   * @param spark sparkSession to use for json parsing
   * @param df Dataframe containing column to parse
   * @param c column name as a string -- supports recursion via dot map notation parent.child.grandchild
   * @return
   */
  def structFromJson(spark: SparkSession, df: DataFrame, c: String): Column = {
    import spark.implicits._
    require(SchemaTools.getAllColumnNames(df.schema).contains(c), s"The dataframe does not contain col $c")
    require(df.select(SchemaTools.flattenSchema(df): _*).schema.fields.map(_.name).contains(c.replaceAllLiterally(".", "_")), "Column must be a json formatted string")
    val jsonSchema = spark.read.json(df.select(col(c)).filter(col(c).isNotNull).as[String]).schema
    if (jsonSchema.fields.map(_.name).contains("_corrupt_record")) {
      println(s"WARNING: The json schema for column $c was not parsed correctly, please review.")
    }
    if (jsonSchema.isEmpty) {
      lit(null)
    } else {
      from_json(col(c), jsonSchema).alias(c)
    }
  }

  // TODO -- Remove keys with nulls from maps?
  //  Add test to ensure that null/"" key and null/"" value are both handled
  //  as of 0.4.1 failed with key "" in spark_conf
  //  TEST for multiple null/"" cols / keynames in same struct/record

  /**
   * Converts a struct field to a map of string,string
   * @param df Dataframe containing column to convert
   * @param colToConvert name of struct column to be converted from struct to map
   *                     must reference a struct field and the fields contained in the struct must be simple types,
   *                     types that can be implicitly cast to a string
   * @param dropEmptyKeys whether or not to remove keys that have a null value. This is defaulted to true as
   *                      it's most common to remove keys without a value and there are often many of these
   * @return
   */
  def structToMap(df: DataFrame, colToConvert: String, dropEmptyKeys: Boolean = true): Column = {

    val mapColName = colToConvert.split("\\.").takeRight(1).head
    val dfFlatColumnNames = getAllColumnNames(df.schema)
    if (dfFlatColumnNames.exists(_.startsWith(colToConvert))) { // if column exists within schema
      df.select(colToConvert).schema.fields.head.dataType.typeName match {
        case "struct" =>
          val schema = df.select(s"${colToConvert}.*").schema
          val mapCols = collection.mutable.LinkedHashSet[Column]()
          schema.fields.foreach(field => {
            val kRaw = field.name.trim
            val k = if (kRaw.isEmpty || kRaw == "") {
              val errMsg = s"SCHEMA WARNING: Column $colToConvert is being converted to a map but has a null key value. " +
                s"This key value will be replaced with a 'null_<random_string>' but should be corrected."
              logger.log(Level.WARN, errMsg)
              println(errMsg)
              s"null_${randomString(22L, 6)}"
            } else kRaw
            mapCols.add(lit(k).cast("string"))
            mapCols.add(col(s"${colToConvert}.${field.name}").cast("string"))
          })
          val newRawMap = map(mapCols.toSeq: _*)
          if (dropEmptyKeys) {
            when(newRawMap.isNull, lit(null).cast(MapType(StringType, StringType, valueContainsNull = true)))
              .otherwise(map_filter(newRawMap, (_,v) => v.isNotNull)).alias(mapColName)
          } else newRawMap.alias(mapColName)
        case "null" =>
          lit(null).cast(MapType(StringType, StringType, valueContainsNull = true)).alias(mapColName)
        case x =>
          throw new Exception(s"function structToMap, columnToConvert must be of type struct but found $x instead")
      }
    } else { // colToConvert doesn't exist within the schema return null map type
      lit(null).cast(MapType(StringType, StringType, valueContainsNull = true)).alias(mapColName)
    }
  }

  def randomString(seed: Long, length: Int = 10): String = {
    val r = new Random(seed) // Using seed to reuse suffixes on continuous duplicates
    r.alphanumeric.take(length).mkString("")
  }

  def uniqueRandomStrings(uniquesNeeded: Option[Int] = None, seed: Option[Long] = None, length: Int = 10): Seq[String] = {
    (0 to uniquesNeeded.getOrElse(500) + 10)
      .map(i => randomString(seed.getOrElse(new Random().nextLong()) + i, length))
      .distinct
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
    val unsupportedMapCast = s"SCHEMA ERROR: Column: ${fullColName}\nImplicit casting of map types is not supported."

    // if one field is complex and the other is not, fail
    //noinspection TypeCheckCanBeMatch
    if (oneComplexOneNot) unsupportedComplexCastingMsg
    else if (requiredTypeName == "map" || fieldTypeName == "map") unsupportedMapCast
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

  /**
   * Build validationRunner for Schema validation. Provides the input schema vs. the minimum required schema
   * to ensure rules are met. Input schema must conform to the minimum required schema or otherwise be able to
   * be implicitly altered to meet the requirements
   * @param dfSchema source schema to validate
   * @param minRequiredSchema minimum required schema against which to validate
   *                          fields of null type will be removed from the schema, this is different than
   *                          enforce nonNullableColumns
   * @param enforceNonNullCols Whether or not to implicitly convert nonNullable columns identified in the
   *                           minimum schema where the input schema may otherwise allow nulls
   * @param isDebug if enabled enables more robust logging
   * @param cPrefix defaults to None -- used to map location within the schema through recursion
   * @return
   */
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

  /**
   * validates the minimum required schema against the schema provided in the validator
   * all rules in the validator must be met to return a ValidatedColumn
   * @param validator Validation rule set and input schema
   * @param cPrefix defaults to None -- used to map location within the schema through recursion
   * @param enforceNonNullCols Whether or not to implicitly convert nonNullable columns identified in the
   *                           minimum schema where the input schema may otherwise allow nulls
   * @param isDebug if enabled enables more robust logging
   * @return
   */
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

        case dt: MapType => // field is MapType
          val dtMap = dt.asInstanceOf[MapType]
          if (!requiredFieldStructure.dataType.isInstanceOf[MapType])
            throw malformedStructureHandler(fieldStructure, requiredFieldStructure, cPrefix)

          // map key or value doesn't match FAIL
          if (
            dtMap.keyType != requiredFieldStructure.dataType.asInstanceOf[MapType].keyType ||
              dtMap.valueType != requiredFieldStructure.dataType.asInstanceOf[MapType].valueType
          ) throw malformedStructureHandler(fieldStructure, requiredFieldStructure, cPrefix)

          // build and return map(k,v)
          validator

        case dt: ArrayType => // field is ArrayType
          val dtArray = dt.asInstanceOf[ArrayType]
          // field is array BUT requirement is NOT array --> throw error
          if (!requiredFieldStructure.dataType.isInstanceOf[ArrayType])
            throw malformedStructureHandler(fieldStructure, requiredFieldStructure, cPrefix)

          // array[complexType]
          // if elementType is nested complex, recurse to validate and return array of validated children
          dtArray.elementType match {
            case eType: StructType =>
              val requiredElementType = requiredFieldStructure.dataType.asInstanceOf[ArrayType].elementType
              // Required element type must be a structType if actual field elementType is structType
              // as explicit cast to StructType is required
              if (!requiredElementType.isInstanceOf[StructType])
                throw malformedStructureHandler(fieldStructure, requiredFieldStructure, cPrefix)
              val validatedChildren = buildValidationRunner(
                eType,
                requiredElementType.asInstanceOf[StructType],
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

