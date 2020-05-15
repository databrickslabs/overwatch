package com.databricks.labs.overwatch
import java.io.IOException

import com.databricks.labs.overwatch.utils.{DataTarget, OverwatchParams, TokenSecret}
import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.ArrayNode

import scala.collection.mutable.ArrayBuffer

// TODO -- Handle master nodes that don't exist
class ParamDeserializer() extends StdDeserializer[OverwatchParams](classOf[OverwatchParams]) {

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

    val dataTarget = DataTarget(
      Some(masterNode.get("dataTarget").get("databaseName").asText()),
      Some(masterNode.get("dataTarget").get("databaseLocation").asText()))

    val auditLogPath = masterNode.get("auditLogPath").asText()

    val badRecordsPath = masterNode.get("badRecordsPath").asText()

    val overwatchScopes = ArrayBuffer[String]()
    val overwatchScopesNode = masterNode.get("overwatchScope").asInstanceOf[ArrayNode].elements()
    while (overwatchScopesNode.hasNext) {
      overwatchScopes.append(overwatchScopesNode.next().asText())
    }

//    {\"tokenSecret\":{\"scope\":\"tomes\",\"key\":\"main\"},\"dataTarget\":null}
//    {\"tokenSecret\":{\"scope\":\"tomes\",\"key\":\"main\"},\"dataTarget\":{\"databaseName\":\"Overwatch\",\"databaseLocation\":null}}

    OverwatchParams(token, Some(dataTarget), Some(auditLogPath),
      Some(badRecordsPath), Some(overwatchScopes.toArray.toSeq))
  }
}
