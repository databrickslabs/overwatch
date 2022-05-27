package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.log4j.{Level, Logger}

/**
 * SecretTools handles common functionality related to working with secrets:
 * 1) Get Databricks API token stored in specified secret
 * 2) Normalize secret structure to be stored at Delta table pipeline_report under inputConfig.tokenSecret nested struct column
 * There are two secret types available now - AWS Secrets Manager, Databricks secrets
 */
trait SecretTools[T <: TokenSecretContainer] {
  def getApiToken : String
  def getTargetTableStruct: TokenSecret
}

object SecretTools {
  private val logger: Logger = Logger.getLogger(this.getClass)
  type DatabricksTokenSecret = TokenSecret

  private class DatabricksSecretTools(tokenSecret : DatabricksTokenSecret) extends SecretTools[DatabricksTokenSecret] {
    override def getApiToken: String = {
      val scope = tokenSecret.scope
      val key = tokenSecret.key
      val authMessage = s"Executing with Databricks token located in secret, scope=$scope : key=$key"
      logger.log(Level.INFO, authMessage)
      dbutils.secrets.get(scope, key)
    }

    override def getTargetTableStruct: TokenSecret = {
      TokenSecret(tokenSecret.scope,tokenSecret.key)
    }
  }

  private class AwsSecretTools(tokenSecret : AwsTokenSecret) extends SecretTools[AwsTokenSecret] {
    override def getApiToken: String = {
      val secretId = tokenSecret.secretId
      val region = tokenSecret.region
      val tokenKey = tokenSecret.tokenKey
      val authMessage = s"Executing with AWS token located in secret, secretId=$secretId : region=$region : tokenKey=$tokenKey"
      logger.log(Level.INFO, authMessage)
      AwsSecrets.readApiToken(secretId, region, tokenSecret.tokenKey)
    }

    override def getTargetTableStruct: TokenSecret = {
      TokenSecret(tokenSecret.region, tokenSecret.secretId)
    }
  }

  def apply(secretSource: TokenSecretContainer): SecretTools[_] = {
    secretSource match {
      case x: AwsTokenSecret => new AwsSecretTools(x)
      case y: DatabricksTokenSecret => new DatabricksSecretTools(y)
      case _ => throw new IllegalArgumentException(s"${secretSource.toString} not implemented")
    }
  }
}
