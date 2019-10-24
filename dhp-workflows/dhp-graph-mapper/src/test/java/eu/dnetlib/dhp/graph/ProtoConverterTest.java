package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Publication;
import org.apache.commons.io.IOUtils;
import static org.junit.Assert.*;
import org.junit.Test;
import scala.tools.nsc.doc.model.Public;

public class ProtoConverterTest {


    @Test
    public void convertDatasourceTest() throws Exception {
        final String json = IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/graph/organization.json"));

        Oaf result = ProtoConverter.convert(json);

        assertNotNull(result);
        assertTrue(result instanceof Datasource);
        Datasource ds = (Datasource) result;
        assertNotNull(ds.getId());

        System.out.println(ds.getId());




    }

    @Test
    public void convertPublicationTest() throws Exception {
        final String json = IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/graph/publication.json"));

        Oaf result = ProtoConverter.convert(json);

        assertNotNull(result);
        assertTrue(result instanceof Publication);
        Publication p = (Publication) result;

        System.out.println(p);

    }

}
