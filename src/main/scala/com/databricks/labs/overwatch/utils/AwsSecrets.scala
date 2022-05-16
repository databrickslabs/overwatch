package com.databricks.labs.overwatch.utils

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.log4j.{Level, Logger}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.parse

import java.util.Base64

object AwsSecrets {

  private val logger: Logger = Logger.getLogger(this.getClass)
  //println(readRawSecretFromAws("data-eng-secret","us-east-2"))

  def readApiToken(secretId: String, region: String, apiTokenKey: String = "apiToken"): String = {
    secretValueAsMap(secretId, region)
      .getOrElse(apiTokenKey ,throw new IllegalStateException("apiToken param not found"))
      .asInstanceOf[String]
  }

  def secretValueAsMap(secretId: String, region: String = "us-east-2"): Map[String, Any] =
    parseJsonToMap(readRawSecretFromAws(secretId,region))

  def readRawSecretFromAws(secretId: String, region: String): String = {
    logger.log(Level.INFO,s"Looking up secret $secretId in AWS Secret Manager")

    val secretsClient = AWSSecretsManagerClientBuilder
      .standard()
      .withRegion(region)
      .build()
    val request = new GetSecretValueRequest().withSecretId(secretId)
    val secretValue = secretsClient.getSecretValue(request)

    if (secretValue.getSecretString != null)
      secretValue.getSecretString
    else
      new String(Base64.getDecoder.decode(secretValue.getSecretBinary).array)
  }

  def parseJsonToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }
}
