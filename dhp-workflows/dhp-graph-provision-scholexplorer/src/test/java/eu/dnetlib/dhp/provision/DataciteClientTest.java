package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.provision.scholix.Scholix;
import eu.dnetlib.dhp.provision.scholix.ScholixResource;
import eu.dnetlib.dhp.provision.update.*;
import eu.dnetlib.scholexplorer.relation.RelationMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;


public class DataciteClientTest {


    @Test
    public void dataciteSCholixTest() throws Exception {
        final String json = IOUtils.toString(getClass().getResourceAsStream("datacite.json"));
        final RelationMapper mapper = RelationMapper.load();

        Datacite2Scholix ds = new Datacite2Scholix(mapper);
        final List<Scholix> s = ds.generateScholixFromJson(json);
        System.out.println(new ObjectMapper().writeValueAsString(s));
    }



    public void testClient() throws Exception {
        RetrieveUpdateFromDatacite.main(new String[]{
                "-n", "file:///data/new_s2.txt",
                "-t", "/data/new_s2.txt",
                "-ts", "1585760736",
                "-ih", "ip-90-147-167-25.ct1.garrservices.it",
                "-in", "datacite",
        });


        SparkResolveScholixTarget.main(new String[]{
                "-s", "file:///data/new_s.txt",
                "-m", "local[*]",
                "-w", "/data/scholix/provision",
                "-h", "ip-90-147-167-25.ct1.garrservices.it",

        });
    }


    public void testResolveDataset() throws Exception {
        DataciteClient dc  = new DataciteClient("ip-90-147-167-25.ct1.garrservices.it");
        ScholixResource datasetByDOI = dc.getDatasetByDOI("10.17182/hepdata.15392.v1/t5");
        Assertions.assertNotNull(datasetByDOI);
        System.out.println(new ObjectMapper().writeValueAsString(datasetByDOI));


        CrossrefClient cr = new CrossrefClient("ip-90-147-167-25.ct1.garrservices.it");
        ScholixResource crossrefByDOI =  cr.getResourceByDOI("10.26850/1678-4618eqj.v35.1.2010.p41-46");
        Assertions.assertNotNull(crossrefByDOI);
        System.out.println(new ObjectMapper().writeValueAsString(crossrefByDOI));



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
}
