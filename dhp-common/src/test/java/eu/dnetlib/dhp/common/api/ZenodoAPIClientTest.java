package eu.dnetlib.dhp.common.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

public class ZenodoAPIClientTest {

    private final String URL_STRING = "https://sandbox.zenodo.org/api/deposit/depositions";
    private final String ACCESS_TOKEN = "5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe";

    private final String CONCEPT_REC_ID = "657113";



    @Test
    public void testNewDeposition() throws IOException {

        ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
                ACCESS_TOKEN);
        Assertions.assertEquals(201, client.newDeposition());

        File file = new File(getClass()
                .getResource("/eu/dnetlib/dhp/common/api/newDeposition")
                .getPath());

        InputStream is = new FileInputStream(file);

        Assertions.assertEquals(200,client.uploadIS(is, "first_deposition", file.length()));


        String metadata = "{\"metadata\":{\"access_right\":\"open\",\"creators\":[{\"affiliation\":\"ISTI - CNR\",\"name\":\"Baglioni, Miriam\",\"orcid\":\"0000-0002-2273-9004\"}],\"description\":\"\\u003cp\\u003eThis is a test for the automatic upload of files in a new deposition\\u003c/p\\u003e \",\"title\":\"Test.\",\"upload_type\":\"other\",\"version\":\"1.0\"}}";

        Assertions.assertEquals(200, client.sendMretadata(metadata));

        Assertions.assertEquals(202, client.publish());

    }


    @Test
    public void testNewVersionNewName() throws IOException, MissingConceptDoiException {


        ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
                ACCESS_TOKEN);

        Assertions.assertEquals(201, client.newVersion(CONCEPT_REC_ID));

        File file = new File(getClass()
                .getResource("/eu/dnetlib/dhp/common/api/newVersion")
                .getPath());

        InputStream is = new FileInputStream(file);

        Assertions.assertEquals(200, client.uploadIS(is, "newVersion_deposition", file.length()));


        Assertions.assertEquals(202, client.publish());

    }

    @Test
    public void testNewVersionOldName() throws IOException, MissingConceptDoiException {


        ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
                ACCESS_TOKEN);

        Assertions.assertEquals(201, client.newVersion(CONCEPT_REC_ID));

        File file = new File(getClass()
                .getResource("/eu/dnetlib/dhp/common/api/newVersion2")
                .getPath());

        InputStream is = new FileInputStream(file);

        Assertions.assertEquals(200, client.uploadIS(is, "newVersion_deposition", file.length()));


        Assertions.assertEquals(202, client.publish());

    }

}
