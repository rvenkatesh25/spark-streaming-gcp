// from https://github.com/GoogleCloudPlatform/cloud-bigtable-examples
package com.example.prodspark.client

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.http.HttpBackOffIOExceptionHandler
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler
import com.google.api.client.http.HttpRequest
import com.google.api.client.http.HttpRequestInitializer
import com.google.api.client.http.HttpResponse
import com.google.api.client.http.HttpUnsuccessfulResponseHandler
import com.google.api.client.util.ExponentialBackOff
import com.google.api.client.util.Sleeper
import org.slf4j.LoggerFactory

/**
  * THIS WAS TAKEN FROM THE CMDLINE-PULL EXAMPLE OF GOOGLE CLOUD PLATFORM
  * and translated into scala:
  * https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-java/tree/master/cmdline-pull
  *
  * RetryHttpInitializerWrapper will automatically retry upon RPC
  * failures, preserving the auto-refresh behavior of the Google
  * Credentials.
  *
  * @param wrappedCredential Intercepts the request for filling in the "Authorization"
  *    header field, as well as recovering from certain unsuccessful
  *    error codes wherein the Credential must refresh its token for a
  *    retry
  */
class RetryHttpInitializerWrapper(wrappedCredential: Credential) extends HttpRequestInitializer {
  /**
    *  One minutes in milliseconds.
    */
  private val ONEMINUTE = 60000

  /**
    *  A sleeper; you can replace it with a mock in your test.
    */
  private val sleeper: Sleeper = Sleeper.DEFAULT

  override def initialize(request: HttpRequest) {
    request.setReadTimeout(2 * ONEMINUTE) // 2 minutes read timeout
    val backoffHandler =
      new HttpBackOffUnsuccessfulResponseHandler(
        new ExponentialBackOff())
          .setSleeper(sleeper)

    request.setInterceptor(wrappedCredential)

    request.setUnsuccessfulResponseHandler(
      new HttpUnsuccessfulResponseHandler() {
        override def handleResponse(
          request: HttpRequest,
          response: HttpResponse,
          supportsRetry: Boolean
        ): Boolean = {
          if (wrappedCredential.handleResponse(request, response, supportsRetry)) {
            // If credential decides it can handle it,
            // the return code or message indicated
            // something specific to authentication,
            // and no backoff is desired.
            true
          } else if (backoffHandler.handleResponse(request, response, supportsRetry)) {
            // Otherwise, we defer to the judgement of
            // our internal backoff handler.
            log.info("Retrying "+ request.getUrl.toString)
            true
          } else {
            false
          }
        }
      })
    request.setIOExceptionHandler(
      new HttpBackOffIOExceptionHandler(new ExponentialBackOff()).setSleeper(sleeper))
  }

  private val log = LoggerFactory.getLogger(this.getClass)
}
