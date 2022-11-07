package eu.dnetlib.dhp.datacite

import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

abstract class AbstractRestClient extends Iterator[String] {

  var buffer: List[String] = List()
  var current_index: Int = 0

  var scroll_value: Option[String] = None

  var complete: Boolean = false

  def extractInfo(input: String): Unit

  protected def getBufferData(): Unit

  def doHTTPGETRequest(url: String): String = {
    val httpGet = new HttpGet(url)
    doHTTPRequest(httpGet)

  }

  def doHTTPPOSTRequest(url: String, json: String): String = {
    val httpPost = new HttpPost(url)
    if (json != null) {
      val entity = new StringEntity(json)
      httpPost.setEntity(entity)
      httpPost.setHeader("Accept", "application/json")
      httpPost.setHeader("Content-type", "application/json")
    }
    doHTTPRequest(httpPost)
  }

  def hasNext: Boolean = {
    buffer.nonEmpty && current_index < buffer.size
  }

  override def next(): String = {
    val next_item: String = buffer(current_index)
    current_index = current_index + 1
    if (current_index == buffer.size)
      getBufferData()
    next_item
  }

  private def doHTTPRequest[A <: HttpUriRequest](r: A): String = {
    val timeout = 600; // seconds
    val config = RequestConfig
      .custom()
      .setConnectTimeout(timeout * 1000)
      .setConnectionRequestTimeout(timeout * 1000)
      .setSocketTimeout(timeout * 1000)
      .build()
    val client = HttpClientBuilder.create().setDefaultRequestConfig(config).build()
    try {
      var tries = 4
      while (tries > 0) {
        println(s"requesting ${r.getURI}")
        try {
          val response = client.execute(r)
          println(s"get response with status${response.getStatusLine.getStatusCode}")
          if (response.getStatusLine.getStatusCode > 400) {
            tries -= 1
          } else
            return IOUtils.toString(response.getEntity.getContent)
        } catch {
          case e: Throwable =>
            println(s"Error on requesting ${r.getURI}")
            e.printStackTrace()
            tries -= 1
        }
      }
      ""
    } finally {
      if (client != null)
        client.close()
    }
  }

  getBufferData()
}
