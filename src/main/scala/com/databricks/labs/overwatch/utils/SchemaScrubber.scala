package com.databricks.labs.overwatch.utils

import com.databricks.labs.overwatch.utils.SchemaScrubber.SanitizedField
import com.databricks.labs.overwatch.utils.SchemaTools.uniqueRandomStrings
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

class SchemaScrubber(
                      sanitizationRules: List[SanitizeRule],
                      sanitizationExceptions: Array[SanitizeFieldException]
                    ) extends SparkSessionWrapper {
  private val logger: Logger = Logger.getLogger(this.getClass)
  // TODO -- Delta writer is schema case sensitive and will fail on write if column case is not identical on both sides
  //  As such, schema case sensitive validation needs to be enabled and a handler for whether to assume the same data
  //  and merge the data, or drop it, or quarantine it or what. This is very common in cases where a column is of
  //  struct type but the key's are derived via user-input (i.e. event log "properties" field).
  //  UPDATE: This has been handled with spark.conf.get("spark.sql.caseSensitive") but needs to be tested on structs
  // TODO -- throw exception if the resulting string is empty

  /**
   * Sanitize the field names using the list of sanitization rules passed in. These fields are processed in the
   * order of which they are received in the list.
   *
   * @param s
   * @return
   */
  private def sanitizeFieldName(s: String, rules: List[SanitizeRule]): String = {
    rules.foldLeft(s)((s, r) => {
      val debugLine = s"RUNNING SANITIZE RULE: FIELD $s -- ${r.from} -> ${r.to}"
      logger.debug(debugLine)
      s.replaceAll(r.from, r.to)
    })
  }

  /**
   * Clean field name and recurse according to the sanitization rules provided
   *
   * @param field
   * @return
   */
  private def sanitizeFields(field: StructField, parentSanitizations: List[SanitizeRule] = List[SanitizeRule]()): SanitizedField = {
    require(
      sanitizationExceptions.map(_.field).deep == sanitizationExceptions.map(_.field).distinct.deep,
      s"""DUPLICATE EXCEPTIONS FOUND FOR SAME FIELD(s):
         |${sanitizationExceptions.map(_.field).toList.diff(sanitizationExceptions.map(_.field).toList).map(_.name).mkString(", ")}
         |""".stripMargin
    )
    val fieldException = sanitizationExceptions.find(ex => ex.field == field)
    val recurse = fieldException.exists(_.recursive) || parentSanitizations.nonEmpty
    val sanitizations = if (recurse) { // if exception is set to recursive
      (parentSanitizations ++ fieldException.map(_.rules).getOrElse(sanitizationRules)).distinct
    } else fieldException.map(_.rules).getOrElse(sanitizationRules)

    SanitizedField(
      field,
      field.copy(name = sanitizeFieldName(field.name, sanitizations), dataType = sanitizeSchema(field.dataType, sanitizations))
    )
  }

  /**
   * When working with complex, evolving schemas across MANY versions and platforms, it's common to wind up with bad
   * schemas. After cleansing, schemas can result in multiple columns with idential names
   * which cannot be saved. Overwatch must be resilient to this; thus, uniques are created.
   * A unique suffix to the end of the offending columns and logged to
   * note the issue as a warning as well as print it out in the run log via stdOUT.
   *
   * @param fields Array[SanitizedField] which includes the original field name and the sanitized field name. The
   *               original field name is used for logging and to produce a unique hash before sanitization in the
   *               case of post-sanitization duplicates
   * @return
   */
  private def generateUniques(fields: Array[SanitizedField]): Array[StructField] = {
    val caseSensitive = spark.conf.get("spark.sql.caseSensitive").toBoolean
    val fieldNames = if (caseSensitive) {
      fields.map(_.sanitizedField.name.trim)
    } else fields.map(_.sanitizedField.name.trim.toLowerCase())
    val dups = fieldNames.diff(fieldNames.distinct)
    val dupCount = dups.length
    if (dupCount == 0) { // if no dups, return the sanitized fields
      fields.map(_.sanitizedField)
    } else { // if dups are present, create a hash based on the original (bad / duplicated) field
      // switched to hash (from random string) as of version 0610 to reduce cardinality of unique suffixes
      val warnMsg = s"SCHEMA WARNING: --> The following fields were not unique after schema cleansing and " +
        s"have been renamed in place but should be reviewed.\n" +
        s"DUPLICATE FIELDS:\n" +
        s"${dups.mkString("\n")}"
      println(warnMsg)
      logger.log(Level.WARN, warnMsg)
      fields.map(f => {
        val fieldName = if (caseSensitive) f.sanitizedField.name else f.sanitizedField.name.toLowerCase
        if (dups.contains(fieldName)) {
          val generatedUniqueName = f.sanitizedField.name + "_UNIQUESUFFIX_" + f.originalField.name.hashCode.toString
          val uniqueColumnMapping = s"\n${f.originalField.name} --> ${generatedUniqueName}"
          println(uniqueColumnMapping)
          logger.log(Level.WARN, uniqueColumnMapping)
          f.sanitizedField.copy(name = generatedUniqueName)
        }
        else f.sanitizedField
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
  private def sanitizeSchema(dataType: DataType, parentSanitizations: List[SanitizeRule] = List()): DataType = {
    dataType match {
      case dt: StructType =>
        val dtStruct = dt.asInstanceOf[StructType]
        dtStruct.copy(fields = generateUniques(dtStruct.fields.map(f => sanitizeFields(f, parentSanitizations))))
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
   *  not sure if this will be done, need to review further
   * @param df Input dataframe to be cleansed
   * @return
   */
  def scrubSchema(df: DataFrame): DataFrame = {
    spark.createDataFrame(df.rdd, sanitizeSchema(df.schema).asInstanceOf[StructType])
  }

}

object SchemaScrubber {

  case class SanitizedField(originalField: StructField, sanitizedField: StructField)

  private val _defaultSanitizationRules: List[SanitizeRule] = List[SanitizeRule](
    SanitizeRule("\\s", ""),
    SanitizeRule("[^a-zA-Z0-9]", "_"),
    SanitizeRule("_UNIQUESUFFIX_", "_UNIQUESUFFIX__")
  )

  private val _noExceptions: Array[SanitizeFieldException] = Array()

  /**
   * Initialize the SchemaScrubber with the following values or allow them to take on the Overwatch defaults
   * @param sanitizationRules List of GLOBAL sanitization rules to be processed in the order by which they should be
   *                          processed. The order applied will be from lowest index to highest.
   * @param exceptions Array of SanitizeFieldException. The rules list will be applied in the same order
   *                   as the default sanitization rules, from lowest to highest list index.
   * @return
   */
  def apply(
             sanitizationRules: List[SanitizeRule] = _defaultSanitizationRules,
             exceptions: Array[SanitizeFieldException] = _noExceptions
           ): SchemaScrubber = {
    new SchemaScrubber(
      sanitizationRules, exceptions
    )
  }

  def scrubSchema(df: DataFrame): DataFrame = {
    apply().scrubSchema(df)
  }

}
