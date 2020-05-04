package eu.dnetlib.dhp.schema.oaf;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class MeasureTest {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @Test
    public void testMeasureSerialization() throws IOException {

        Measure m = new Measure();

        m.setId("popularity");
        m.setUnit(Lists.newArrayList(
                unit("score", "0.5")));

        String s = OBJECT_MAPPER.writeValueAsString(m);
        System.out.println(s);

        Measure mm = OBJECT_MAPPER.readValue(s, Measure.class);

        Assertions.assertNotNull(mm);
    }

    private KeyValue unit(String key, String value) {
        KeyValue unit = new KeyValue();
        unit.setKey(key);
        unit.setValue(value);
        return unit;
    }

}
