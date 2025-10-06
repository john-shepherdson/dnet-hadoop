package eu.dnetlib.dhp.oa.provision;

import eu.dnetlib.dhp.sx.provision.ConvertScholixResourceToES;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;

public class TestConverter {


    @Test
    public void test() throws Exception {

        ConvertScholixResourceToES c = new ConvertScholixResourceToES("summary");
        String json =IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/provision/json/resource.json"), Charset.defaultCharset());

        System.out.println(json);

        c.apply(json);
    }
}
