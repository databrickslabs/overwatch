package com.databricks.labs.overwatch
import java.io.IOException

import com.databricks.labs.overwatch.utils.{AuditLogConfig, AzureAuditLogEventhubConfig, DataTarget, OverwatchParams, TokenSecret}
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.ArrayNode

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// TODO -- Handle master nodes that don't exist
class ParamDeserializer() extends StdDeserializer[OverwatchParams](classOf[OverwatchParams]) {

  private def getNodeFromPath(parentNode: JsonNode, path: String): Option[JsonNode] = {
    val pathArray = path.split("\\.")
    try {
      pathArray.foldLeft(Some(parentNode)) {
        case (subNode, layer) =>
          Some(subNode.get.get(layer))
//          subNode.get(layer)
      }
//      Some(lowestNode)
    } catch {
      case e: Throwable => {
        println(s"CANNOT FIND NODE AT PATH ${path}", e)
        None
      }
    }
  }

  private def getOption[T](node: JsonNode, path: String, default: T): Option[T] = {
    val pathArray = path.split("\\.")
    val lookupKey = pathArray.last
    val lowestNode = if (pathArray.length > 1) {
      val nodeLookup = pathArray.dropRight(1).mkString("\\.")
      getNodeFromPath(node, nodeLookup)
    } else None

    if (lowestNode.nonEmpty) {
      if (lowestNode.get.has(lookupKey)) {
        default match {
          case _: Boolean => Some(lowestNode.get.get(lookupKey).asBoolean().asInstanceOf[T])
          case _: Double => Some(lowestNode.get.get(lookupKey).asDouble().asInstanceOf[T])
          case _: Int => Some(lowestNode.get.get(lookupKey).asInt().asInstanceOf[T])
          case _: Long => Some(lowestNode.get.get(lookupKey).asLong().asInstanceOf[T])
          case _ => Some(lowestNode.get.get(lookupKey).asText().asInstanceOf[T])
        }
      } else None
    } else None

  }

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
        getOption(masterNode, "dataTarget.databaseLocation", "")
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
    val moveProcessedFiles = masterNode.get("migrateProcessedEventLogs").asBoolean(false)

//    {\"tokenSecret\":{\"scope\":\"tomes\",\"key\":\"main\"},\"dataTarget\":null}
//    {\"tokenSecret\":{\"scope\":\"tomes\",\"key\":\"main\"},\"dataTarget\":{\"databaseName\":\"Overwatch\",\"databaseLocation\":null}}

    OverwatchParams(
      auditLogConfig,
      token,
      dataTarget,
      Some(badRecordsPath),
      Some(overwatchScopes.toArray.toSeq),
      moveProcessedFiles)
  }
}