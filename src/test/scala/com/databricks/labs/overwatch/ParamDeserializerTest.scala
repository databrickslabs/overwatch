package com.databricks.labs.overwatch

import com.databricks.labs.overwatch.utils.{AuditLogConfig, AzureAuditLogEventhubConfig, DatabricksContractPrices, OverwatchParams}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.scalatest.funspec.AnyFunSpec


class ParamDeserializerTest extends AnyFunSpec {

  describe("ParamDeserializer") {

    it("should decode incomplete parameters") {
      val incomplete = """
        |{"auditLogConfig":{"azureAuditLogEventhubConfig":{"connectionString":"test","eventHubName":"overwatch-evhub",
        |"auditRawEventsPrefix":"/tmp/overwatch_dev/overwatch_etl_dev","maxEventsPerTrigger":10001}},
        |"badRecordsPath":"/tmp/overwatch_dev/overwatch_etl_dev/sparkEventsBadrecords",
        |"overwatchScope":["audit","accounts","jobs","sparkEvents","clusters","clusterEvents","notebooks","pools"],
        |"maxDaysToLoad":60,
        |"databricksContractPrices":{"interactiveDBUCostUSD":0.56,"automatedDBUCostUSD":0.26}}
        |""".stripMargin

      val paramModule: SimpleModule = new SimpleModule()
        .addDeserializer(classOf[OverwatchParams], new ParamDeserializer)
      val mapper: ObjectMapper with ScalaObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
        .registerModule(DefaultScalaModule)
        .registerModule(paramModule)
        .asInstanceOf[ObjectMapper with ScalaObjectMapper]

      val expected = OverwatchParams(
        AuditLogConfig(
          azureAuditLogEventhubConfig = Some(AzureAuditLogEventhubConfig(
            connectionString = "test",
            eventHubName = "overwatch-evhub",
            auditRawEventsPrefix = "/tmp/overwatch_dev/overwatch_etl_dev",
            maxEventsPerTrigger = 10001
          ))
        ),
        None,
        None,
        Some("/tmp/overwatch_dev/overwatch_etl_dev/sparkEventsBadrecords"),
        Some(Seq("audit","accounts","jobs","sparkEvents","clusters","clusterEvents","notebooks","pools")),
        60,
        DatabricksContractPrices(0.56, 0.26),
        None
      )
      assertResult(expected)(mapper.readValue[OverwatchParams](incomplete))

    }
  }
}
