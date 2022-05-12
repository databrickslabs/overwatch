package com.databricks.labs.overwatch.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

trait SecretTools[T <: TokenSecretContainer] {
  def getApiToken(tokenSecret: T) : String
}

object SecretTools {

  def getApiToken2[T <: TokenSecretContainer](tokenSecret: T, getSecretStrategy: (T => String)): String = {
    getSecretStrategy(tokenSecret)
  }

  type DatabricksTokenSecret = TokenSecret
  private class DatabricksSecretHolder extends SecretTools[DatabricksTokenSecret] {
    override def getApiToken(tokenSecret : DatabricksTokenSecret): String =
      dbutils.secrets.get(tokenSecret.scope, tokenSecret.key)
  }

  private class AwsSecretHolder extends SecretTools[AwsTokenSecret] {
    override def getApiToken(tokenSecret : AwsTokenSecret): String =
      AwsSecrets.readApiToken(tokenSecret.secretId, tokenSecret.region)
  }

  def getToken(secretSource: TokenSecretContainer): String = {
    secretSource match {
      case x: AwsTokenSecret => new AwsSecretHolder().getApiToken(x)
      case y: DatabricksTokenSecret => new DatabricksSecretHolder().getApiToken(y)
      case _ => throw new IllegalArgumentException(s"${secretSource.toString} not implemented")
    }
  }
}
