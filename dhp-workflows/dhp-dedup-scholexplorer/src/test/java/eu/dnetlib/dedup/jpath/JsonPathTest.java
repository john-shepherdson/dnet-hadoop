package eu.dnetlib.dedup.jpath;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import java.util.List;
import java.util.Map;

public class JsonPathTest {

    @Test
    public void testJPath () throws  Exception {
        final String json = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dedup/conf/sample.json"));
        List<Map<String, Object>> pid = JsonPath.read(json, "$.pid[*]");
//        System.out.println(json);

        pid.forEach(it -> {
            try {
                System.out.println(new ObjectMapper().writeValueAsString(it));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });




    }
}
