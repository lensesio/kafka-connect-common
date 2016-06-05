package com.datamountaineer.streamreactor.connect.config

import javax.net.ssl.{KeyManager, SSLContext, TrustManager}

import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

/**
  * Created by andrew@datamountaineer.com on 19/04/16. 
  * stream-reactor
  */
class TestSSLConfigContext extends WordSpec with Matchers with BeforeAndAfter {
  var sslConfig : SSLConfig = null
  var sslConfigNoClient : SSLConfig = null

  before {
    val trustStorePath = System.getProperty("truststore")
    val trustStorePassword ="erZHDS9Eo0CcNo"
    val keystorePath = System.getProperty("keystore")
    val keystorePassword ="8yJQLUnGkwZxOw"
    sslConfig = SSLConfig(trustStorePath, trustStorePassword , Some(keystorePath), Some(keystorePassword), true)
    sslConfigNoClient = SSLConfig(trustStorePath, trustStorePassword , Some(keystorePath), Some(keystorePassword), false)
  }

  "SSLConfigContext" should {
    "should return an Array of KeyManagers" in {
      val keyManagers = SSLConfigContext.getKeyManagers(sslConfig)
      keyManagers.length shouldBe 1
      val entry = keyManagers.head
      entry shouldBe a [KeyManager]
    }

    "should return an Array of TrustManagers" in {
      val trustManager = SSLConfigContext.getTrustManagers(sslConfig)
      trustManager.length shouldBe 1
      val entry = trustManager.head
      entry shouldBe a [TrustManager]
    }

    "should return a SSLContext" in {
      val context = SSLConfigContext(sslConfig)
      context.getProtocol shouldBe "SSL"
      context shouldBe a [SSLContext]
    }
  }
}
