
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.utils.DHPUtils;

public class IdentifierFactoryTest {

	private static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Test
	public void testCreateIdentifierForPublication() throws IOException {

		verifyIdentifier(
			"publication_doi1.json", "50|DansKnawCris::0829b5191605bdbea36d6502b8c1ce1f", false);
		verifyIdentifier(
			"publication_doi2.json", "50|doi_________::" + DHPUtils.md5("10.1016/j.cmet.2010.03.013"), true);
		verifyIdentifier("publication_pmc1.json", "50|pmc_________::" + DHPUtils.md5("21459329"), true);
		verifyIdentifier(
			"publication_urn1.json",
			"50|urn_________::" + DHPUtils.md5("urn:nbn:nl:ui:29-f3ed5f9e-edf6-457e-8848-61b58a4075e2"), true);

		final String defaultID = "50|DansKnawCris::0829b5191605bdbea36d6502b8c1ce1f";
		verifyIdentifier("publication_3.json", defaultID, true);
		verifyIdentifier("publication_4.json", defaultID, true);
		verifyIdentifier("publication_5.json", defaultID, true);
	}

	@Test
	public void testCreateIdentifierForPublicationNoHash() throws IOException {

		verifyIdentifier("publication_doi1.json", "50|doi_________::10.1016/j.cmet.2011.03.013", false);
		verifyIdentifier("publication_doi2.json", "50|doi_________::10.1016/j.cmet.2010.03.013", false);
		verifyIdentifier("publication_pmc1.json", "50|pmc_________::21459329", false);
		verifyIdentifier(
			"publication_urn1.json", "50|urn_________::urn:nbn:nl:ui:29-f3ed5f9e-edf6-457e-8848-61b58a4075e2", false);

		final String defaultID = "50|DansKnawCris::0829b5191605bdbea36d6502b8c1ce1f";
		verifyIdentifier("publication_3.json", defaultID, false);
		verifyIdentifier("publication_4.json", defaultID, false);
		verifyIdentifier("publication_5.json", defaultID, false);
	}

	protected void verifyIdentifier(String filename, String expectedID, boolean md5) throws IOException {
		final String json = IOUtils.toString(getClass().getResourceAsStream(filename));
		final Publication pub = OBJECT_MAPPER.readValue(json, Publication.class);

		String id = IdentifierFactory.createIdentifier(pub, md5);

		assertNotNull(id);
		assertEquals(expectedID, id);
	}

}
