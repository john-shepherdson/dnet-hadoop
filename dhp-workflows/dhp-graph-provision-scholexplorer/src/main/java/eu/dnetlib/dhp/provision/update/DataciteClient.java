package eu.dnetlib.dhp.provision.update;

import eu.dnetlib.dhp.provision.scholix.ScholixResource;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

public class DataciteClient {


    private String host;
    private String index ="datacite";
    private String indexType = "dump";
    private Datacite2Scholix d2s;

    public DataciteClient(String host) {
        this.host = host;

        d2s = new Datacite2Scholix(null);
        d2s.setRootPath("$._source.attributes");
    }

    public Iterable<String> getDatasetsFromTs(final Long timestamp) {
        return ()-> {
            try {
                return new DataciteClientIterator(host, index, timestamp);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };


    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public ScholixResource getDatasetByDOI(final String doi) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(String.format("http://%s:9200/%s/%s/%s", host, index,indexType, doi.replaceAll("/","%2F")));
            CloseableHttpResponse response = client.execute(httpGet);
            final String json =IOUtils.toString(response.getEntity().getContent());
            return d2s.generateDataciteScholixResource(json);
        } catch (Throwable e) {
            return null;
        }
    }


}
