package eu.dnetlib.doiboost;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class DoiBoostTest {

    @Test
    @Ignore
    public void test() throws Exception {
        //SparkDownloadContentFromCrossref.main(null);
        CrossrefImporter.main(new String[]{
                "-n","file:///tmp",
                "-t","file:///tmp/p.seq",
        });
    }


    @Test
    public void testPath() throws Exception {
        final String json = IOUtils.toString(getClass().getResourceAsStream("response.json"));

        final List<String > res = JsonPath.read(json, "$.hits.hits[*]._source.blob");



        System.out.println(res.size());

    }


    @Test
    @Ignore
    public void testParseResponse() throws IOException {
        long end, start = System.currentTimeMillis();
        ESClient client = new ESClient("ip-90-147-167-25.ct1.garrservices.it", "crossref");


        int i = 0;
        while (client.hasNext()) {
            Assert.assertNotNull(client.next());
            i++;
            if(i % 1000 == 0) {
                end = System.currentTimeMillis();
                System.out.println("Vel 1000 records in "+((end -start)/1000)+"s");
                start = System.currentTimeMillis();
            }


            if (i >1000000)
                break;
        }
    }

}
