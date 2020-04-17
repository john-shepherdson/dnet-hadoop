package eu.dnetlib.doiboost;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.util.List;

public class DoiBoostTest {


    @Test
    public void testPath() throws Exception {
        final String json = IOUtils.toString(getClass().getResourceAsStream("response.json"));

        final List<String> res = JsonPath.read(json, "$.hits.hits[*]._source.blob");


        System.out.println(res.size());

    }


}
