package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.provision.scholix.Scholix;
import eu.dnetlib.scholexplorer.relation.RelationMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
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


    @Test
    public void testClient() throws Exception {
        DataciteClient client = new DataciteClient("ip-90-147-167-25.ct1.garrservices.it","datacite",1585454082);
        int i = 0;
        final RelationMapper mapper = RelationMapper.load();

        Datacite2Scholix ds = new Datacite2Scholix(mapper);
        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/sandro/new_s.txt"));

        final ObjectMapper m  = new ObjectMapper();





        while (client.hasNext()){
            i ++;


            final String next = client.next();
            try {
                final List<Scholix> res = ds.generateScholixFromJson(next);
                if (res!= null)
                res
                        .forEach(
                                s -> {
                                    try {

                                        writer.write(m.writeValueAsString(s));
                                        writer.write("\n");
                                    } catch (Throwable e) {
                                        throw new RuntimeException(e);
                                    }
                                }


                        );
            }catch (Throwable t) {
                System.out.println(next);
                throw new RuntimeException(t);
            }
            if(i %1000 == 0) {
                System.out.println("added "+i);
            }
        }
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
