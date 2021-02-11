package com.databricks.labs.overwatch

import java.io.IOException

import com.databricks.labs.overwatch.utils.{AuditLogConfig, AzureAuditLogEventhubConfig, DataTarget, DatabricksContractPrices, OverwatchParams, TokenSecret}
import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.ArrayNode

import scala.collection.mutable.ArrayBuffer
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
    val getLowestNode = Try {
    val pathArray = path.split("\\.")
      pathArray.foldLeft(parentNode) {
        case (subNode, layer) =>
          if (subNode.has(layer))
            subNode.get(layer)
          else throw new NullPointerException
        //          subNode.get(layer)
      }
      //      Some(lowestNode)
    }
    getLowestNode match {
      case Success(lowestNode) => Some(lowestNode)
      case Failure(_) => None
    }
  }

  /**
   * Best method I could find for to successfully and consistently get optional json nodes that may be null and/or may
   * be non-existent as keys. This helper function handles several types and can be extended to handle more types.
   * Additionally, it greatly cleans up the forever ongoing code of drilling into a path to get an optional node by
   * some type as it takes a dot delimited path coordinate (such as level1.level2.nodeName) and handles all the
   * digging for the node. I'm certain there's a better/easier way to do this but couldn't find it.
   * @param node top level JsonNode in which to look
   * @param path dot-delimited coordinates path relative to the node (i.e. layer1.layer2.key)
   * @param default default value if not found or is null, like "getOrElse" functions. The default type is also used
   *                to implicitly cast the output to the same type
   * @tparam T Type of default, return type will match this
   * @return
   */
  private def getOption[T](node: JsonNode, path: String, default: T): Option[T] = {
    val nodeValue = Try {
      val pathArray = path.split("\\.")
      val lookupKey = pathArray.last
      val nodeLookup = pathArray.dropRight(1).mkString("\\.")
      val lowestNode = getNodeFromPath(node, nodeLookup).get
      default match {
        case _: Boolean => lowestNode.get(lookupKey).asBoolean().asInstanceOf[T]
        case _: Double => lowestNode.get(lookupKey).asDouble().asInstanceOf[T]
        case _: Int => lowestNode.get(lookupKey).asInt().asInstanceOf[T]
        case _: Long => lowestNode.get(lookupKey).asLong().asInstanceOf[T]
        case _ => lowestNode.get(lookupKey).asText().asInstanceOf[T]
      }
    }
    nodeValue match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
    }

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


    val rawAuditPath = getOption(masterNode, "auditLogConfig.rawAuditPath", "")
    val azureEventHubNode = getNodeFromPath(masterNode, "auditLogConfig.azureAuditLogEventhubConfig")


    val azureAuditEventHubConfig = if (azureEventHubNode.nonEmpty) {
      Some(AzureAuditLogEventhubConfig(
        azureEventHubNode.get.get("connectionString").asText(""),
        azureEventHubNode.get.get("eventHubName").asText(""),
        azureEventHubNode.get.get("auditRawEventsPrefix").asText(""),
        azureEventHubNode.get.get("maxEventsPerTrigger").asInt(10000),
        getOption(azureEventHubNode.get, "auditRawEventsChk", ""),
        getOption(azureEventHubNode.get, "auditLogChk", "")
      ))
    } else None

    val auditLogConfig = AuditLogConfig(rawAuditPath, azureAuditEventHubConfig)

    val dataTarget = if (masterNode.has("dataTarget")) {
      Some(DataTarget(
        getOption(masterNode, "dataTarget.databaseName", ""),
        getOption(masterNode, "dataTarget.databaseLocation", ""),
        getOption(masterNode, "dataTarget.consumerDatabaseName", ""),
        getOption(masterNode, "dataTarget.consumerDatabaseLocation", "")
      ))
    } else None
    //      Some(masterNode.get("dataTarget").get("databaseName").asText()),
    //      Some(masterNode.get("dataTarget").get("databaseLocation").asText()))

    val badRecordsPath = masterNode.get("badRecordsPath").asText()

    val overwatchScopes = ArrayBuffer[String]()
    val overwatchScopesNode = masterNode.get("overwatchScope").asInstanceOf[ArrayNode].elements()
    while (overwatchScopesNode.hasNext) {
      overwatchScopes.append(overwatchScopesNode.next().asText())
    }
    val maxDaysToLoad = masterNode.get("maxDaysToLoad").asInt(60)

    val dbContractPrices = DatabricksContractPrices(
      getOption(masterNode, "databricksContractPrices.interactiveDBUCostUSD",0.56 ).getOrElse(0.56),
      getOption(masterNode, "databricksContractPrices.automatedDBUCostUSD",0.26 ).getOrElse(0.26)
    )

    val primordialDateString = masterNode.get("primordialDateString").asText()

    //    {\"tokenSecret\":{\"scope\":\"tomes\",\"key\":\"main\"},\"dataTarget\":null}
    //    {\"tokenSecret\":{\"scope\":\"tomes\",\"key\":\"main\"},\"dataTarget\":{\"databaseName\":\"Overwatch\",\"databaseLocation\":null}}

    OverwatchParams(
      auditLogConfig,
      token,
      dataTarget,
      Some(badRecordsPath),
      Some(overwatchScopes.toArray.toSeq),
      maxDaysToLoad,
      dbContractPrices,
      Some(primordialDateString)
    )
  }
}