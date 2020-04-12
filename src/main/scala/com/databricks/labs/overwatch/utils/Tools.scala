package com.databricks.labs.overwatch.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.core.io.JsonStringEncoder
import java.util.UUID

import javax.crypto
import javax.crypto.KeyGenerator
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec}


object JsonUtils {

  case class JsonStrings(prettyString: String, compactString: String, escapedString: String, fromObj: Any)

  private[overwatch] lazy val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  private val encoder = JsonStringEncoder.getInstance

  def jsonToMap(message: String): Map[String, Any] = {
    objectMapper.readValue(message, classOf[Map[String, Any]])
  }

  def objToJson(obj: Any): JsonStrings = {
    JsonStrings(
      objectMapper.writerWithDefaultPrettyPrinter.writeValueAsString(obj),
      objectMapper.writeValueAsString(obj),
      new String(encoder.quoteAsString(objectMapper.writeValueAsString(obj))),
      obj
    )
  }

}

class Cipher {

  private val tempKey = UUID.randomUUID().toString
  private val salt = Array[Byte](16)
  private val spec = new PBEKeySpec(tempKey.toCharArray, salt, 1000, 128 * 8)
  private val keyGenner = KeyGenerator.getInstance("AES")
  keyGenner.init(128)
  private val aesKey = keyGenner.generateKey()
  private val iv = new IvParameterSpec("0102030405060708".getBytes())
  private val cipher = crypto.Cipher.getInstance("AES/CBC/PKCS5Padding")

  private[overwatch] def encrypt(text: String): Array[Byte] = {
    //    val aesKey = new SecretKeySpec(tempKey.getBytes(), "AES")
    //    val aesKey = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1").generateSecret(spec)
    cipher.init(crypto.Cipher.ENCRYPT_MODE, aesKey, iv)
    cipher.doFinal(text.getBytes())
  }

  private[overwatch] def decrypt(byteStream: Array[Byte]): String = {
    cipher.init(crypto.Cipher.DECRYPT_MODE, aesKey, iv)
    new String(cipher.doFinal(byteStream))
  }


}