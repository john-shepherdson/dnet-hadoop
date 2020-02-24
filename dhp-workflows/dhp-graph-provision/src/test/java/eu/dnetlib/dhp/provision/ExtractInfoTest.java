package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.ScholixSummary;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

public class ExtractInfoTest {

    @Test
    public void test() throws Exception {

        final String json = IOUtils.toString(getClass().getResourceAsStream("record.json"));


        ProvisionUtil.getItemType(json,ProvisionUtil.TARGETJSONPATH);

    }


    @Test
    public void testSerialization() throws Exception {

        ScholixSummary summary = new ScholixSummary();
        summary.setDescription("descrizione");
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(summary);
        System.out.println(json);
        System.out.println(mapper.readValue(json, ScholixSummary.class).getDescription());
    }


    @Test
    @Ignore
    public void testIndex() throws Exception {
        SparkIndexCollectionOnES.main(

                new String[] {
                        "-mt", "local[*]",
                        "-s", "/home/sandro/dli",
                        "-i", "dli_object"
                }
        );
    }
}
