package eu.dnetlib.dhp.graph;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;

public class MappingUtilsTest {

    private MappingUtils utils;

    @Before
    public void setUp() {
        utils = new MappingUtils();
    }

    @Test
    public void testOafMappingDatasource() throws IOException {

        final InputStreamReader in = new InputStreamReader(getClass().getResourceAsStream("datasource.json"));
        final EntityRelEntity e = new ObjectMapper().readValue(in, EntityRelEntity.class);
        e.getSource().setType("datasource");

        final EntityRelEntity out = utils.pruneModel(e);
        System.out.println(out);

    }

    @Test
    public void testOafMappinResult() throws IOException {

        final InputStreamReader in = new InputStreamReader(getClass().getResourceAsStream("result.json"));
        final EntityRelEntity e = new ObjectMapper().readValue(in, EntityRelEntity.class);
        e.getSource().setType("otherresearchproduct");

        final EntityRelEntity out = utils.pruneModel(e);
        System.out.println(out);

    }
}
