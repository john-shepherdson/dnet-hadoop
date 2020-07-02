
package eu.dnetlib.doiboost.orcidnodoi.xml;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.text.Normalizer;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ximpleware.NavException;
import com.ximpleware.ParseException;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.doiboost.orcid.model.AuthorData;
import eu.dnetlib.doiboost.orcidnodoi.model.Contributor;
import eu.dnetlib.doiboost.orcidnodoi.model.WorkDataNoDoi;
import eu.dnetlib.doiboost.orcidnodoi.similarity.AuthorMatcher;
import jdk.nashorn.internal.ir.annotations.Ignore;

public class OrcidNoDoiTest {

	private static final Logger logger = LoggerFactory.getLogger(OrcidNoDoiTest.class);

	String nameA = "Khairy";
	String surnameA = "Abdel Dayem";
	String otherNameA = "Dayem MKA";
	String nameB = "K";
	String surnameB = "Abdel-Dayem";
	String orcidIdA = "0000-0003-2760-1191";

	@Test
	@Ignore
	private void readPublicationFieldsTest()
		throws IOException, XPathEvalException, XPathParseException, NavException, VtdException, ParseException {
		logger.info("running loadPublicationFieldsTest ....");
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream("activity_work_0000-0003-2760-1191.xml"));

		if (xml == null) {
			logger.info("Resource not found");
		}
		XMLRecordParserNoDoi p = new XMLRecordParserNoDoi();
		if (p == null) {
			logger.info("XMLRecordParserNoDoi null");
		}
		WorkDataNoDoi workData = null;
		try {
			workData = p.VTDParseWorkData(xml.getBytes());
		} catch (Exception e) {
			logger.error("parsing xml", e);
		}
		assertNotNull(workData);
		assertNotNull(workData.getOid());
		logger.info("oid: " + workData.getOid());
		assertNotNull(workData.getTitles());
		logger.info("titles: ");
		workData.getTitles().forEach(t -> {
			logger.info(t);
		});
		logger.info("source: " + workData.getSourceName());
		logger.info("type: " + workData.getType());
		logger.info("urls: ");
		workData.getUrls().forEach(u -> {
			logger.info(u);
		});
		logger.info("publication date: ");
		workData.getPublicationDates().forEach(d -> {
			logger.info(d.getYear() + " - " + d.getMonth() + " - " + d.getDay());
		});
		logger.info("external id: ");
		workData.getExtIds().removeIf(e -> e.getRelationShip() != null && !e.getRelationShip().equals("self"));
		workData.getExtIds().forEach(e -> {
			logger.info(e.getType() + " - " + e.getValue() + " - " + e.getRelationShip());
		});
		logger.info("contributors: ");
		workData.getContributors().forEach(c -> {
			logger
				.info(
					c.getName() + " - " + c.getRole() + " - " + c.getSequence());
		});

	}

	@Test
	@Ignore
	private void authorMatchTest() throws Exception {
		logger.info("running authorSimpleMatchTest ....");
		String orcidWork = "activity_work_0000-0003-2760-1191-similarity.xml";
		AuthorData author = new AuthorData();
		author.setName(nameA);
		author.setSurname(surnameA);
		author.setOid(orcidIdA);
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream(orcidWork));

		if (xml == null) {
			logger.info("Resource not found");
		}
		XMLRecordParserNoDoi p = new XMLRecordParserNoDoi();
		if (p == null) {
			logger.info("XMLRecordParserNoDoi null");
		}
		WorkDataNoDoi workData = null;
		try {
			workData = p.VTDParseWorkData(xml.getBytes());
		} catch (Exception e) {
			logger.error("parsing xml", e);
		}
		assertNotNull(workData);
		AuthorMatcher.match(author, workData.getContributors());
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		logger.info(gson.toJson(workData));
	}
}
