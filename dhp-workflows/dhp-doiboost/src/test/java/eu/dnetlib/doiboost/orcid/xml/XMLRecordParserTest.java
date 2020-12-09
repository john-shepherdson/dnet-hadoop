
package eu.dnetlib.doiboost.orcid.xml;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.dhp.schema.orcid.AuthorSummary;
import eu.dnetlib.dhp.schema.orcid.Work;
import eu.dnetlib.dhp.schema.orcid.WorkDetail;
import eu.dnetlib.doiboost.orcid.OrcidClientTest;
import eu.dnetlib.doiboost.orcid.SparkDownloadOrcidWorks;
import eu.dnetlib.doiboost.orcid.model.WorkData;
import eu.dnetlib.doiboost.orcidnodoi.json.JsonWriter;
import eu.dnetlib.doiboost.orcidnodoi.xml.XMLRecordParserNoDoi;

public class XMLRecordParserTest {
	private static final String NS_WORK = "work";
	private static final String NS_WORK_URL = "http://www.orcid.org/ns/work";
	private static final String NS_COMMON_URL = "http://www.orcid.org/ns/common";
	private static final String NS_COMMON = "common";
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
		OrcidClientTest.logToFile(OBJECT_MAPPER.writeValueAsString(authorData));
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
	private void testOrcidOtherNamesXMLParser() throws Exception {

		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("summary_0000-0001-5109-1000_othername.xml"));
		AuthorData authorData = XMLRecordParser.VTDParseAuthorData(xml.getBytes());
		assertNotNull(authorData);
		assertNotNull(authorData.getOtherNames());
		assertTrue(authorData.getOtherNames().get(0).equals("Andrew C. Porteus"));
		String jsonData = JsonWriter.create(authorData);
		assertNotNull(jsonData);
	}

	@Test
	private void testWorkIdLastModifiedDateXMLParser() throws Exception {
		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("record_0000-0001-5004-5918.xml"));
		Map<String, String> workIdLastModifiedDate = XMLRecordParser.retrieveWorkIdLastModifiedDate(xml.getBytes());
		workIdLastModifiedDate.forEach((k, v) -> {
			try {
				OrcidClientTest
					.logToFile(
						k + " " + v + " isModified after " + SparkDownloadOrcidWorks.lastUpdateValue + ": "
							+ SparkDownloadOrcidWorks.isModified("0000-0001-5004-5918", v));
			} catch (IOException e) {
			}
		});
	}

	@Test
	public void testAuthorSummaryXMLParser() throws Exception {
		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("record_0000-0001-5004-5918.xml"));
		AuthorSummary authorSummary = XMLRecordParser.VTDParseAuthorSummary(xml.getBytes());
		authorSummary.setBase64CompressData(ArgumentApplicationParser.compressArgument(xml));
		OrcidClientTest.logToFile(JsonWriter.create(authorSummary));
	}

	@Test
	public void testWorkDataXMLParser() throws Exception {
		String xml = IOUtils
			.toString(
				this.getClass().getResourceAsStream("activity_work_0000-0003-2760-1191.xml"));
		WorkDetail workDetail = XMLRecordParserNoDoi.VTDParseWorkData(xml.getBytes());
		Work work = new Work();
		work.setWorkDetail(workDetail);
		work.setBase64CompressData(ArgumentApplicationParser.compressArgument(xml));
		OrcidClientTest.logToFile(JsonWriter.create(work));
	}
}
