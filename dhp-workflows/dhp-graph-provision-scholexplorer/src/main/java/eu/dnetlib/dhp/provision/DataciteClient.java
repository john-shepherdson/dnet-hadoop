package eu.dnetlib.dhp.provision;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class DataciteClient implements Iterator<String> {

    final static String blobPath = "$.hits.hits[*]._source";
    final static String scrollIdPath = "$._scroll_id";

    String scrollId;

    List<String> buffer;

    final String esHost;
    final String esIndex;
    final ObjectMapper mapper = new ObjectMapper();

    public DataciteClient(final String esHost, final String esIndex, final long timestamp) throws IOException {

        this.esHost = esHost;
        this.esIndex = esIndex;
        final String body =getResponse(String.format("http://%s:9200/%s/_search?scroll=1m", esHost, esIndex), String.format("{\"size\":1000, \"query\":{\"range\":{\"timestamp\":{\"gte\":%d}}}}", timestamp));
        scrollId= getJPathString(scrollIdPath, body);
        buffer = getBlobs(body);

    }


    public String getResponse(final String url,final String json ) {
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
        JSONArray array = JsonPath.read(body, blobPath);
        return array.stream().map(
                o -> {
                    try {
                        return mapper.writeValueAsString(o);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                }
                ).collect(Collectors.toList());
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
