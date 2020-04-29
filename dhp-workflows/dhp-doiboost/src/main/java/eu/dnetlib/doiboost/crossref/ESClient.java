package eu.dnetlib.doiboost.crossref;

import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESClient implements Iterator<String> {
  private static final Logger logger = LoggerFactory.getLogger(ESClient.class);

  static final String blobPath = "$.hits[*].hits[*]._source.blob";
  static final String scrollIdPath = "$._scroll_id";
  static final String JSON_NO_TS = "{\"size\":1000}";
  static final String JSON_WITH_TS =
      "{\"size\":1000, \"query\":{\"range\":{\"timestamp\":{\"gte\":%d}}}}";
  static final String JSON_SCROLL = "{\"scroll_id\":\"%s\",\"scroll\" : \"1m\"}";

  private final String scrollId;

  private List<String> buffer;

  private final String esHost;

  public ESClient(final String esHost, final String esIndex) throws IOException {

    this.esHost = esHost;
    final String body =
        getResponse(
            String.format("http://%s:9200/%s/_search?scroll=1m", esHost, esIndex), JSON_NO_TS);
    scrollId = getJPathString(scrollIdPath, body);
    buffer = getBlobs(body);
  }

  public ESClient(final String esHost, final String esIndex, final long timestamp)
      throws IOException {
    this.esHost = esHost;
    final String body =
        getResponse(
            String.format("http://%s:9200/%s/_search?scroll=1m", esHost, esIndex),
            String.format(JSON_WITH_TS, timestamp));
    scrollId = getJPathString(scrollIdPath, body);
    buffer = getBlobs(body);
  }

  private String getResponse(final String url, final String json) {
    CloseableHttpClient client = HttpClients.createDefault();
    try {

      HttpPost httpPost = new HttpPost(url);
      if (json != null) {
        StringEntity entity = new StringEntity(json);
        httpPost.setEntity(entity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
      }
      CloseableHttpResponse response = client.execute(httpPost);

      return IOUtils.toString(response.getEntity().getContent());
    } catch (Throwable e) {
      throw new RuntimeException("Error on executing request ", e);
    } finally {
      try {
        client.close();
      } catch (IOException e) {
        throw new RuntimeException("Unable to close client ", e);
      }
    }
  }

  private String getJPathString(final String jsonPath, final String json) {
    try {
      Object o = JsonPath.read(json, jsonPath);
      if (o instanceof String) return (String) o;
      return null;
    } catch (Exception e) {
      return "";
    }
  }

  private List<String> getBlobs(final String body) {
    final List<String> res = JsonPath.read(body, "$.hits.hits[*]._source.blob");
    return res;
  }

  @Override
  public boolean hasNext() {
    return (buffer != null && !buffer.isEmpty());
  }

  @Override
  public String next() {
    final String nextItem = buffer.remove(0);
    if (buffer.isEmpty()) {

      final String json_param = String.format(JSON_SCROLL, scrollId);
      final String body =
          getResponse(String.format("http://%s:9200/_search/scroll", esHost), json_param);
      try {
        buffer = getBlobs(body);
      } catch (Throwable e) {
        logger.error("Error on  get next page: body:" + body);
      }
    }
    return nextItem;
  }
}
