package com.databricks.labs.overwatch.eventhubs

import org.apache.spark.eventhubs.EventHubsConf

object AadAuthInstance {
  def addAadAuthParams(ehConf: EventHubsConf, aadParams: Map[String, Object]) : EventHubsConf = {
    ehConf.setAadAuthCallbackParams(aadParams).setAadAuthCallback(new AadClientAuthentication(aadParams))
  }
}
