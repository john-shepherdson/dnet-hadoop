package eu.dnetlib.dhp.schema.action;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Relation;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

/** @author claudio.atzori */
public class AtomicActionTest {

    @Test
    public void serializationTest() throws IOException {

        Relation rel = new Relation();
        rel.setSource("1");
        rel.setTarget("2");
        rel.setRelType("resultResult");
        rel.setSubRelType("dedup");
        rel.setRelClass("merges");

        AtomicAction aa1 = new AtomicAction(Relation.class, rel);

        final ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(aa1);

        assertTrue(StringUtils.isNotBlank(json));

        AtomicAction aa2 = mapper.readValue(json, AtomicAction.class);

        assertEquals(aa1.getClazz(), aa2.getClazz());
        assertEquals(aa1.getPayload(), aa2.getPayload());
    }
}
