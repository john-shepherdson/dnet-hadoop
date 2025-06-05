
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Publication;

class IdentifierFactoryTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Test
	void testCreateIdentifierForPublication() throws IOException {

		verifyIdentifier(
			"publication_doi1.json", "50|doi_________::79dbc7a2a56dc1532659f9038843256e", true);

		verifyIdentifier(
			"publication_doi2.json", "50|doi_________::79dbc7a2a56dc1532659f9038843256e", true);

		verifyIdentifier(
			"publication_doi3.json", "50|pmc_________::e2a339e0e11bfbf55462e14a07f1b304", true);

		verifyIdentifier(
			"publication_doi4.json", "50|od______2852::38861c44e6052a8d49f59a4c39ba5e66", true);

		verifyIdentifier(
			"publication_doi5.json", "50|doi_________::3bef95c0ca26dd55451fc8839ea69d27", true);

		verifyIdentifier(
			"publication_pmc1.json", "50|DansKnawCris::0829b5191605bdbea36d6502b8c1ce1f", true);

		verifyIdentifier(
			"publication_pmc2.json", "50|pmc_________::e2a339e0e11bfbf55462e14a07f1b304", true);

		verifyIdentifier(
			"publication_openapc.json", "50|doi_________::79dbc7a2a56dc1532659f9038843256e", true);

		final String defaultID = "50|DansKnawCris::0829b5191605bdbea36d6502b8c1ce1f";
		verifyIdentifier("publication_3.json", defaultID, true);
		verifyIdentifier("publication_4.json", defaultID, true);
		verifyIdentifier("publication_5.json", defaultID, true);

	}

	@Test
	void testCreateIdentifierForPublicationNoHash() throws IOException {

		verifyIdentifier("publication_doi1.json", "50|doi_________::10.1016/j.cmet.2010.03.013", false);
		verifyIdentifier("publication_doi2.json", "50|doi_________::10.1016/j.cmet.2010.03.013", false);
		verifyIdentifier("publication_pmc1.json", "50|DansKnawCris::0829b5191605bdbea36d6502b8c1ce1f", false);
		verifyIdentifier(
			"publication_urn1.json", "50|DansKnawCris::0829b5191605bdbea36d6502b8c1ce1f", false);

		final String defaultID = "50|DansKnawCris::0829b5191605bdbea36d6502b8c1ce1f";
		verifyIdentifier("publication_3.json", defaultID, false);
		verifyIdentifier("publication_4.json", defaultID, false);
		verifyIdentifier("publication_5.json", defaultID, false);
	}

	@Test
	void testCreateIdentifierForROHub() throws IOException {
		verifyIdentifier(
			"orp-rohub.json", "50|w3id________::afc7592914ae190a50570db90f55f9c2", true);
	}

	protected void verifyIdentifier(String filename, String expectedID, boolean md5) throws IOException {
		final String json = IOUtils.toString(getClass().getResourceAsStream(filename));
		final Publication pub = OBJECT_MAPPER.readValue(json, Publication.class);

		String id = IdentifierFactory.createIdentifier(pub, md5);
		System.out.println(id);
		assertNotNull(id);
		assertEquals(expectedID, id);
	}

}
