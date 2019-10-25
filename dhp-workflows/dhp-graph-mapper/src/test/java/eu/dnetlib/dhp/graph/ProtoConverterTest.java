package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Organization;
import org.apache.commons.io.IOUtils;

import static org.junit.Assert.*;

import org.junit.Test;

public class ProtoConverterTest {


    @Test
    public void convertDatasourceTest() throws Exception {
        final String json = IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/graph/datasource.json"));

        Oaf result = ProtoConverter.convert(json);
        assertNotNull(result);
        assertTrue(result instanceof Datasource);
        Datasource ds = (Datasource) result;
        assertNotNull(ds.getId());

        System.out.println(ds.getId());


        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writeValueAsString(result));
    }


    @Test
    public void convertOrganizationTest() throws Exception {

        final String json = IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/graph/organization.json"));

        Oaf result = ProtoConverter.convert(json);
        assertNotNull(result);
        assertTrue(result instanceof Organization);
        Organization ds = (Organization) result;
        assertNotNull(ds.getId());

        System.out.println(ds.getId());


        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writeValueAsString(result));

    }

}
