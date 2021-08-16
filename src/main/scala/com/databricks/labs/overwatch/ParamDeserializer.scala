package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.utils._
import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode}

import java.io.IOException
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Custom deserializer to convert json string coming from jobs main class into validated, strongly typed object
 * called OverwatchParams
 */
class ParamDeserializer() extends StdDeserializer[OverwatchParams](classOf[OverwatchParams]) {

  /**
   * Helper function used to search for the node by dot-delimited path such as node1.node2.key and supports
   * optionals and nulls
   * @param parentNode JsonNode from which to look
   * @param path dot-delimited coordinates path relative to the node (i.e. layer1.layer2.key)
   * @return
   */
  private def getNodeFromPath(parentNode: JsonNode, path: String): Option[JsonNode] = {
    if (path.isEmpty) {
      Some(parentNode)
    } else {
      val getLowestNode = Try {
        val pathArray = path.split("\\.")
        pathArray.foldLeft(parentNode) {
          case (subNode, layer) =>
            if (subNode.has(layer))
              subNode.get(layer)
            else
              throw new NullPointerException
        }
      }
      getLowestNode match {
        case Success(lowestNode) => Some(lowestNode)
        case Failure(_) => None
      }
    }
  }

  /**
   * Best method I could find for to successfully and consistently get optional json nodes that may be null and/or may
   * be non-existent as keys. This helper function provides a base for extractor functions for specific types.
   * Additionally, it greatly cleans up the forever ongoing code of drilling into a path to get an optional node by
   * some type as it takes a dot delimited path coordinate (such as level1.level2.nodeName) and handles all the
   * digging for the node. I'm certain there's a better/easier way to do this but couldn't find it.
   * @param node top level JsonNode in which to look
   * @param path dot-delimited coordinates path relative to the node (i.e. layer1.layer2.key)
   * @tparam T Type of default, return type will match this
   * @return
   */
  private def getOption[T](node: JsonNode, path: String, convertFunc: JsonNode => T): Option[T] = {
    val nodeValue = Try {
      val pathArray = path.split("\\.")
      val lookupKey = pathArray.last
      val nodeLookup = pathArray.dropRight(1).mkString("\\.")
      val lowestNode = getNodeFromPath(node, nodeLookup).get
      convertFunc(lowestNode.get(lookupKey))
    }
    nodeValue match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

  def getOptionString(node: JsonNode, path: String): Option[String] = getOption(node, path, x => x.asText())
  def getOptionInt(node: JsonNode, path: String): Option[Int] = getOption(node, path, x => x.asInt())
  def getOptionLong(node: JsonNode, path: String): Option[Long] = getOption(node, path, x => x.asLong())
  def getOptionBoolean(node: JsonNode, path: String): Option[Boolean] = getOption(node, path, x => x.asBoolean())
  def getOptionDouble(node: JsonNode, path: String): Option[Double] = getOption(node, path, x => x.asDouble())

  /**
   * Implementation of deserialize method to return OverwatchParams
   * @param jp
   * @param ctxt
   * @throws java.io.IOException
   * @throws com.fasterxml.jackson.core.JsonProcessingException
   * @return
   */
  @throws(classOf[IOException])
  @throws(classOf[JsonProcessingException])
  override def deserialize(jp: JsonParser, ctxt: DeserializationContext): OverwatchParams = {
    val masterNode = jp.getCodec.readTree[JsonNode](jp)

    val token = try {
      Some(TokenSecret(
        masterNode.get("tokenSecret").get("scope").asText(),
        masterNode.get("tokenSecret").get("key").asText()))
    } catch {
      case e: Throwable =>
        println("No Token Secret Defined", e)
        None
    }

    val rawAuditPath = getOptionString(masterNode, "auditLogConfig.rawAuditPath")
    val auditLogFormat = getOptionString(masterNode, "auditLogConfig.auditLogFormat").getOrElse("json")
    val azureEventHubNode = getNodeFromPath(masterNode, "auditLogConfig.azureAuditLogEventhubConfig")

    val azureAuditEventHubConfig = if (azureEventHubNode.nonEmpty) {
      val node = azureEventHubNode.get
      Option(AzureAuditLogEventhubConfig(
        getOptionString(node, "connectionString").getOrElse(""),
        getOptionString(node, "eventHubName").getOrElse(""),
        getOptionString(node, "auditRawEventsPrefix").getOrElse(""),
        getOptionInt(node, "maxEventsPerTrigger").getOrElse(10000),
        getOptionString(node, "auditRawEventsChk"),
        getOptionString(node, "auditLogChk")
      ))
    } else {
      None
    }

    val auditLogConfig = AuditLogConfig(rawAuditPath, auditLogFormat, azureAuditEventHubConfig)

    val dataTarget = if (masterNode.has("dataTarget")) {
      Some(DataTarget(
        getOptionString(masterNode, "dataTarget.databaseName"),
        getOptionString(masterNode, "dataTarget.databaseLocation"),
        getOptionString(masterNode, "dataTarget.etlDataPathPrefix"),
        getOptionString(masterNode, "dataTarget.consumerDatabaseName"),
        getOptionString(masterNode, "dataTarget.consumerDatabaseLocation")
      ))
    } else {
      None
    }

    val badRecordsPath = getOptionString(masterNode, "badRecordsPath")

    val overwatchScopes = if (masterNode.has("overwatchScope")) {
      val overwatchScopesNode = masterNode.get("overwatchScope")
        .asInstanceOf[ArrayNode].elements()
      Option(asScalaIterator(overwatchScopesNode).map(_.asText()).toArray.toSeq)
    } else {
      None
    }

    val maxDaysToLoad = getOptionInt(masterNode, "maxDaysToLoad").getOrElse(60)

    // Defaulted to list pricing as of June 07 2021
    val dbContractPrices = DatabricksContractPrices(
      getOptionDouble(masterNode, "databricksContractPrices.interactiveDBUCostUSD").getOrElse(0.55),
      getOptionDouble(masterNode, "databricksContractPrices.automatedDBUCostUSD").getOrElse(0.15),
      getOptionDouble(masterNode, "databricksContractPrices.sqlComputeDBUCostUSD").getOrElse(0.22),
      getOptionDouble(masterNode, "databricksContractPrices.jobsLightDBUCostUSD").getOrElse(0.10),
    )

    val primordialDateString = getOptionString(masterNode, "primordialDateString")
    val intelligentScalingConfig = IntelligentScaling(
      getOptionBoolean(masterNode, "intelligentScaling.enabled").getOrElse(false),
      getOptionInt(masterNode, "intelligentScaling.minimumCores").getOrElse(4),
      getOptionInt(masterNode, "intelligentScaling.maximumCores").getOrElse(512),
      getOptionDouble(masterNode, "intelligentScaling.coeff").getOrElse(1.0)
    )

    OverwatchParams(
      auditLogConfig,
      token,
      dataTarget,
      badRecordsPath,
      overwatchScopes,
      maxDaysToLoad,
      dbContractPrices,
      primordialDateString,
      intelligentScalingConfig
    )
  }
}