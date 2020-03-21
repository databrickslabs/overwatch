package com.databricks.labs.overwatch
import java.io.IOException

import com.databricks.labs.overwatch.utils.GlobalStructures._
import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer

class ParamDeserializer() extends StdDeserializer[OverwatchParams](classOf[OverwatchParams]) {

  @throws(classOf[IOException])
  @throws(classOf[JsonProcessingException])
  override def deserialize(jp: JsonParser, ctxt: DeserializationContext): OverwatchParams = {
    val masterNode = jp.getCodec.readTree[JsonNode](jp)

    val token = TokenSecret(
      Some(masterNode.get("tokenSecret").get("scope").asText()),
      Some(masterNode.get("tokenSecret").get("key").asText()))
//["{\"tokenSecret\":{\"scope\":\"tomes\",\"key\":\"main\"}}"]
    OverwatchParams(Some(token))
  }
}
