
package eu.dnetlib.doiboost.orcid.xml;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.doiboost.orcid.model.WorkData;
import eu.dnetlib.doiboost.orcidnodoi.json.JsonWriter;

public class XMLRecordParserTest {

	@Test
	private void testOrcidAuthorDataXMLParser() throws Exception {

		String xml = IOUtils.toString(this.getClass().getResourceAsStream("summary_0000-0001-6828-479X.xml"));

		XMLRecordParser p = new XMLRecordParser();

		AuthorData authorData = p.VTDParseAuthorData(xml.getBytes());
		assertNotNull(authorData);
		assertNotNull(authorData.getName());
		System.out.println("name: " + authorData.getName());
		assertNotNull(authorData.getSurname());
		System.out.println("surname: " + authorData.getSurname());
	}

	@Test
	private void testOrcidXMLErrorRecordParser() throws Exception {

		String xml = IOUtils.toString(this.getClass().getResourceAsStream("summary_error.xml"));

		XMLRecordParser p = new XMLRecordParser();

		AuthorData authorData = p.VTDParseAuthorData(xml.getBytes());
		assertNotNull(authorData);
		assertNotNull(authorData.getErrorCode());
		System.out.println("error: " + authorData.getErrorCode());
	}

	@Test
	private void testOrcidWorkDataXMLParser() throws Exception {

		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("activity_work_0000-0003-2760-1191.xml"));

		XMLRecordParser p = new XMLRecordParser();

		WorkData workData = p.VTDParseWorkData(xml.getBytes());
		assertNotNull(workData);
		assertNotNull(workData.getOid());
		System.out.println("oid: " + workData.getOid());
		assertNotNull(workData.getDoi());
		System.out.println("doi: " + workData.getDoi());
	}

	@Test
	public void testOrcidOtherNamesXMLParser() throws Exception {

		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("summary_0000-0001-5109-1000_othername.xml"));

		XMLRecordParser p = new XMLRecordParser();

		AuthorData authorData = XMLRecordParser.VTDParseAuthorData(xml.getBytes());
		assertNotNull(authorData);
		assertNotNull(authorData.getOtherNames());
		assertTrue(authorData.getOtherNames().get(0).equals("Andrew C. Porteus"));
		String jsonData = JsonWriter.create(authorData);
		assertNotNull(jsonData);
	}
}
