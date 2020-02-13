package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.graph.model.EntityRelEntity;
import eu.dnetlib.dhp.graph.model.RelatedEntity;
import eu.dnetlib.dhp.graph.utils.GraphMappingUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;

public class MappingUtilsTest {

    private GraphMappingUtils utils;

    @Before
    public void setUp() {
        utils = new GraphMappingUtils();
    }

    @Test
    public void testOafMappingDatasource() throws IOException {

        final InputStreamReader in = new InputStreamReader(getClass().getResourceAsStream("datasource.json"));
        final EntityRelEntity e = new ObjectMapper().readValue(in, EntityRelEntity.class);
        e.getSource().setType("datasource");

        final EntityRelEntity out = utils.asRelatedEntity(e);
        System.out.println(out);

    }

    //@Test
    public void testOafMappingResult() throws IOException {

        final InputStreamReader in = new InputStreamReader(getClass().getResourceAsStream("result.json"));
        final EntityRelEntity e = new ObjectMapper().readValue(in, EntityRelEntity.class);

        final EntityRelEntity out = utils.asRelatedEntity(e);
        System.out.println(out);

    }

    @Test
    public void testOafMappingSoftware() throws IOException {

        final InputStreamReader in = new InputStreamReader(getClass().getResourceAsStream("software.json"));
        final EntityRelEntity e = new ObjectMapper().readValue(in, EntityRelEntity.class);

        final EntityRelEntity out = utils.asRelatedEntity(e);
        System.out.println(out);

    }


    @Test
    public void testParseRelatedEntity() throws IOException {

        final InputStreamReader in = new InputStreamReader(getClass().getResourceAsStream("related_entity.json"));
        final RelatedEntity e = new ObjectMapper().readValue(in, RelatedEntity.class);

        System.out.println(e);

    }
}
