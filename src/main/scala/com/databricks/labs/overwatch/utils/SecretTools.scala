package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.log4j.{Level, Logger}

trait SecretTools[T <: TokenSecretContainer] {
  def getApiToken : String
  def getTargetTableStruct: TokenSecret
}

object SecretTools {
  private val logger: Logger = Logger.getLogger(this.getClass)
  type DatabricksTokenSecret = TokenSecret

  private class DatabricksSecretHolder(tokenSecret : DatabricksTokenSecret) extends SecretTools[DatabricksTokenSecret] {
    override def getApiToken: String = {
      val scope = tokenSecret.scope
      val key = tokenSecret.key
      val authMessage = s"Executing with token located in secret, $scope : $key"
      logger.log(Level.INFO, authMessage)
      dbutils.secrets.get(scope, key)
    }

    override def getTargetTableStruct: TokenSecret = {
      TokenSecret(tokenSecret.scope,tokenSecret.key)
    }
  }

  private class AwsSecretHolder(tokenSecret : AwsTokenSecret) extends SecretTools[AwsTokenSecret] {
    override def getApiToken: String = {
      val secretId = tokenSecret.secretId
      val region = tokenSecret.region
      val authMessage = s"Executing with token located in secret, $secretId : $region"
      logger.log(Level.INFO, authMessage)
      AwsSecrets.readApiToken(secretId, region)
    }

    override def getTargetTableStruct: TokenSecret = {
      TokenSecret(tokenSecret.region,tokenSecret.secretId)
    }
  }

  /*
  def getToken(secretSource: TokenSecretContainer): String = {
    secretSource match {
      case x: AwsTokenSecret => new AwsSecretHolder().getApiToken(x)
      case y: DatabricksTokenSecret => new DatabricksSecretHolder().getApiToken(y)
      case _ => throw new IllegalArgumentException(s"${secretSource.toString} not implemented")
    }
  }
   */

  /*
  def getTokenStruct[T <: TokenSecretContainer](tokenSecret: T, getSecretStruct: (T => TokenSecret)): TokenSecret = {
    getSecretStruct(tokenSecret)
  }

  def getSecretTargetStruct(secretSource: TokenSecretContainer): TokenSecret = {
    secretSource match {
      case x: AwsTokenSecret => getTokenStruct(x, (secretSource: AwsTokenSecret) => TokenSecret(x.region,x.secretId)) //new AwsSecretHolder().getTargetTableStruct(x)
      case y: DatabricksTokenSecret => getTokenStruct(y, (secretSource: DatabricksTokenSecret) => TokenSecret(y.scope,y.key)) //new DatabricksSecretHolder().getTargetTableStruct(y)
      case _ => throw new IllegalArgumentException(s"${secretSource.toString} not implemented")
    }
  }
   */

  def apply(secretSource: TokenSecretContainer): SecretTools[_] = {
    secretSource match {
      case x: AwsTokenSecret => new AwsSecretHolder(x)
      case y: DatabricksTokenSecret => new DatabricksSecretHolder(y)
      case _ => throw new IllegalArgumentException(s"${secretSource.toString} not implemented")
    }
  }
}
