package eu.dnetlib.dhp.datacite

import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, JValue}

class DataciteAPIImporter(timestamp: Long = 0, blocks: Long = 10, until: Long = -1) extends AbstractRestClient {

  override def extractInfo(input: String): Unit = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: org.json4s.JValue = parse(input)
    buffer = (json \ "data").extract[List[JValue]].map(s => compact(render(s)))
    val next_url = (json \ "links" \ "next").extractOrElse[String](null)
    scroll_value = if (next_url != null && next_url.nonEmpty) Some(next_url) else None
    if (scroll_value.isEmpty)
      complete = true
    current_index = 0
  }

  def get_url(): String = {
    val to = if (until > 0) s"$until" else "*"
    s"https://api.datacite.org/dois?page[cursor]=1&page[size]=$blocks&query=updated:[$timestamp%20TO%20$to]"

  }

  override def getBufferData(): Unit = {
    if (!complete) {
      val response =
        if (scroll_value.isDefined) doHTTPGETRequest(scroll_value.get)
        else doHTTPGETRequest(get_url())
      extractInfo(response)
    }
  }
}
