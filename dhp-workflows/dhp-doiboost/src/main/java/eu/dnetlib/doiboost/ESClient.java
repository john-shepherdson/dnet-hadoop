package eu.dnetlib.doiboost;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ESClient implements Iterator<String> {

    final static String blobPath = "$.hits[*].hits[*]._source.blob";
    final static String scrollIdPath = "$._scroll_id";

    String scrollId;

    List<String> buffer;

    final String esHost;
    final String esIndex;

    public ESClient(final String esHost, final String esIndex) throws IOException {

        this.esHost = esHost;
        this.esIndex = esIndex;
        final String body =getResponse(String.format("http://%s:9200/%s/_search?scroll=1m", esHost, esIndex), "{\"size\":1000}");
        scrollId= getJPathString(scrollIdPath, body);
        buffer = getBlobs(body);

    }


    private String getResponse(final String url,final String json ) {
        CloseableHttpClient client = HttpClients.createDefault();
        try {

            HttpPost httpPost = new HttpPost(url);
            if (json!= null) {
                StringEntity entity = new StringEntity(json);
                httpPost.setEntity(entity);
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-type", "application/json");
            }
            CloseableHttpResponse response = client.execute(httpPost);

            return IOUtils.toString(response.getEntity().getContent());
        } catch (Throwable e) {
            throw new RuntimeException("Error on executing request ",e);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                throw new RuntimeException("Unable to close client ",e);
            }
        }

    }

    private String  getJPathString(final String jsonPath, final String json) {
        try {
            Object o = JsonPath.read(json, jsonPath);
            if (o instanceof String)
                return (String) o;
            return null;
        } catch (Exception e) {
            return "";
        }
    }

    private List<String> getBlobs(final String body) {
        final List<String > res = JsonPath.read(body, "$.hits.hits[*]._source.blob");
        return res;
    }


    @Override
    public boolean hasNext() {
        return (buffer!= null && !buffer.isEmpty());

    }

    @Override
    public String next() {
        final String nextItem = buffer.remove(0);
        if (buffer.isEmpty()) {
            final String json_param = String.format("{\"scroll_id\":\"%s\",\"scroll\" : \"1m\"}", scrollId);
            final String body =getResponse(String.format("http://%s:9200/_search/scroll", esHost), json_param);
            try {
                buffer = getBlobs(body);
            } catch (Throwable e) {
                System.out.println(body);

            }

        }
        return nextItem;
    }
}
