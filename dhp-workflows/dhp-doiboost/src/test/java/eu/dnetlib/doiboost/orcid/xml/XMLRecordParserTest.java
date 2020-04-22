package eu.dnetlib.doiboost.orcid.xml;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import eu.dnetlib.doiboost.orcid.model.AuthorData;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class XMLRecordParserTest {

    @Test
    public void testOrcidXMLRecordParser() throws Exception {

        String xml =
                IOUtils.toString(
                        this.getClass().getResourceAsStream("summary_0000-0001-6828-479X.xml"));

        XMLRecordParser p = new XMLRecordParser();

        AuthorData authorData = p.VTDParse(xml.getBytes());
        assertNotNull(authorData);
        assertNotNull(authorData.getName());
        System.out.println("name: " + authorData.getName());
        assertNotNull(authorData.getSurname());
        System.out.println("surname: " + authorData.getSurname());
    }

    @Test
    public void testOrcidXMLErrorRecordParser() throws Exception {

        String xml = IOUtils.toString(this.getClass().getResourceAsStream("summary_error.xml"));

        XMLRecordParser p = new XMLRecordParser();

        AuthorData authorData = p.VTDParse(xml.getBytes());
        assertNotNull(authorData);
        assertNotNull(authorData.getErrorCode());
        System.out.println("error: " + authorData.getErrorCode());
    }
}
