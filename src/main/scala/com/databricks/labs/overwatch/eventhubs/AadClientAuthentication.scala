package com.databricks.labs.overwatch.eventhubs

import com.microsoft.aad.msal4j._
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback

import java.util.concurrent.CompletableFuture
import java.util.{Collections, function}
import scala.language.implicitConversions

class AadClientAuthentication(params: Map[String, Object]) extends AadAuthenticationCallback {
  implicit def toJavaFunction[A, B](f: (A) => B): function.Function[A, B] = new java.util.function.Function[A, B] {
    override def apply(a: A): B = f(a)
  }

  override def authority: String = params("aad_tenant_id").asInstanceOf[String]
  val clientId: String = params("aad_client_id").asInstanceOf[String]
  val clientSecret: String = params("aad_client_secret").asInstanceOf[String]
  val authorityEndpoint: String = params.getOrElse("aad_authority_endpoint", "https://login.microsoftonline.com/").asInstanceOf[String]

  override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = try {
    val app = ConfidentialClientApplication
      .builder(clientId, ClientCredentialFactory.createFromSecret(this.clientSecret))
      .authority(authorityEndpoint + authority)
      .build

    val parameters = ClientCredentialParameters.builder(Collections.singleton(audience + ".default")).build

    app.acquireToken(parameters).thenApply((result: IAuthenticationResult) => result.accessToken())
  } catch {
    case e: Exception =>
      val failed = new CompletableFuture[String]
      failed.completeExceptionally(e)
      failed
  }
}

