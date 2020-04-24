package eu.dnetlib.doiboost.orcid.xml;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import eu.dnetlib.doiboost.orcid.model.AuthorData;
import eu.dnetlib.doiboost.orcid.model.WorkData;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class XMLRecordParserTest {

    @Test
    public void testOrcidAuthorDataXMLParser() throws Exception {

        String xml =
                IOUtils.toString(
                        this.getClass().getResourceAsStream("summary_0000-0001-6828-479X.xml"));

        XMLRecordParser p = new XMLRecordParser();

        AuthorData authorData = p.VTDParseAuthorData(xml.getBytes());
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

        AuthorData authorData = p.VTDParseAuthorData(xml.getBytes());
        assertNotNull(authorData);
        assertNotNull(authorData.getErrorCode());
        System.out.println("error: " + authorData.getErrorCode());
    }

    @Test
    public void testOrcidWorkDataXMLParser() throws Exception {

        String xml =
                IOUtils.toString(
                        this.getClass()
                                .getResourceAsStream("activity_work_0000-0002-5982-8983.xml"));

        XMLRecordParser p = new XMLRecordParser();

        WorkData workData = p.VTDParseWorkData(xml.getBytes());
        assertNotNull(workData);
        assertNotNull(workData.getOid());
        System.out.println("oid: " + workData.getOid());
        assertNotNull(workData.getDoi());
        System.out.println("doi: " + workData.getDoi());
    }
}
