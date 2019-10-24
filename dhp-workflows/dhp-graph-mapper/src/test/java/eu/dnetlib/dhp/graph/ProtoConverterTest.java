package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import org.apache.commons.io.IOUtils;
import static org.junit.Assert.*;
import org.junit.Test;

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

}
