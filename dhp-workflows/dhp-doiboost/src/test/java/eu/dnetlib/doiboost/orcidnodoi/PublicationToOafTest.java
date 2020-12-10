
package eu.dnetlib.doiboost.orcidnodoi;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.doiboost.orcidnodoi.oaf.PublicationToOaf;
import jdk.nashorn.internal.ir.annotations.Ignore;

public class PublicationToOafTest {

	private static final Logger logger = LoggerFactory.getLogger(PublicationToOafTest.class);

	@Test
	@Ignore
	private void convertOafPublicationTest() throws Exception {
		String jsonPublication = IOUtils
			.toString(
				PublicationToOafTest.class.getResourceAsStream("publication.json"));
		JsonElement j = new JsonParser().parse(jsonPublication);
		logger.info("json publication loaded: " + j.toString());
		PublicationToOaf publicationToOaf = new PublicationToOaf();
		Publication oafPublication = (Publication) publicationToOaf
			.generatePublicationActionsFromDump(j.getAsJsonObject());
		assertNotNull(oafPublication.getId());
		assertNotNull(oafPublication.getOriginalId());
		assertEquals(oafPublication.getOriginalId().get(0), "60153327");
		logger.info("oafPublication.getId(): " + oafPublication.getId());
		assertEquals(
			oafPublication.getTitle().get(0).getValue(),
			"Evaluation of a percutaneous optical fibre glucose sensor (FiberSense) across the glycemic range with rapid glucoseexcursions using the glucose clamp");
		assertNotNull(oafPublication.getLastupdatetimestamp());
		assertNotNull(oafPublication.getDateofcollection());
		assertNotNull(oafPublication.getDateoftransformation());
		assertTrue(oafPublication.getAuthor().size() == 7);
		oafPublication.getAuthor().forEach(a -> {
			assertNotNull(a.getFullname());
			assertNotNull(a.getRank());
			logger.info("a.getFullname(): " + a.getFullname());
			if (a.getName() != null) {
				logger.info("a.getName(): " + a.getName());
			}
			if (a.getSurname() != null) {
				logger.info("a.getSurname(): " + a.getSurname());
			}
			logger.info("a.getRank(): " + a.getRank());
			if (a.getPid() != null) {
				logger.info("a.getPid(): " + a.getPid().get(0).getValue());
			}

		});
		assertNotNull(oafPublication.getCollectedfrom());
		if (oafPublication.getSource() != null) {
			logger.info((oafPublication.getSource().get(0).getValue()));
		}
		if (oafPublication.getExternalReference() != null) {
			oafPublication.getExternalReference().forEach(e -> {
				assertNotNull(e.getRefidentifier());
				assertEquals(e.getQualifier().getSchemeid(), "dnet:pid_types");
			});
		}
		assertNotNull(oafPublication.getInstance());
		oafPublication.getInstance().forEach(i -> {
			assertNotNull(i.getInstancetype().getClassid());
			logger.info("i.getInstancetype().getClassid(): " + i.getInstancetype().getClassid());
			assertNotNull(i.getInstancetype().getClassname());
			logger.info("i.getInstancetype().getClassname(): " + i.getInstancetype().getClassname());
		});
	}
}
