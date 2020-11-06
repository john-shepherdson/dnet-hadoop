
package eu.dnetlib.doiboost.orcidnodoi.xml;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.text.Normalizer;
import java.util.*;

import javax.validation.constraints.AssertTrue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.junit.jupiter.api.Test;
import org.mortbay.log.Log;
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
	public void readPublicationFieldsTest()
		throws IOException, XPathEvalException, XPathParseException, NavException, VtdException, ParseException {
		logger.info("running loadPublicationFieldsTest ....");
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream("activity_work_0000-0002-2536-4498.xml"));

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
	public void authorMatchTest() throws Exception {
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

		Contributor a = workData.getContributors().get(0);
		assertTrue(a.getCreditName().equals("Abdel-Dayem K"));

		AuthorMatcher.match(author, workData.getContributors());
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		logger.info(gson.toJson(workData));

		assertTrue(workData.getContributors().size() == 6);
		Contributor c = workData.getContributors().get(0);
		assertTrue(c.getOid().equals("0000-0003-2760-1191"));
		assertTrue(c.getName().equals("Khairy"));
		assertTrue(c.getSurname().equals("Abdel Dayem"));
		assertTrue(c.getCreditName().equals("Abdel-Dayem K"));
	}

	@Test
	public void readContributorsTest()
		throws IOException, XPathEvalException, XPathParseException, NavException, VtdException, ParseException {
		logger.info("running loadPublicationFieldsTest ....");
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream("activity_work_0000-0003-2760-1191_contributors.xml"));

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
		assertNotNull(workData.getContributors());
		assertTrue(workData.getContributors().size() == 5);
		assertTrue(StringUtils.isBlank(workData.getContributors().get(0).getCreditName()));
		assertTrue(workData.getContributors().get(0).getSequence().equals("seq0"));
		assertTrue(workData.getContributors().get(0).getRole().equals("role0"));
		assertTrue(workData.getContributors().get(1).getCreditName().equals("creditname1"));
		assertTrue(StringUtils.isBlank(workData.getContributors().get(1).getSequence()));
		assertTrue(StringUtils.isBlank(workData.getContributors().get(1).getRole()));
		assertTrue(workData.getContributors().get(2).getCreditName().equals("creditname2"));
		assertTrue(workData.getContributors().get(2).getSequence().equals("seq2"));
		assertTrue(StringUtils.isBlank(workData.getContributors().get(2).getRole()));
		assertTrue(workData.getContributors().get(3).getCreditName().equals("creditname3"));
		assertTrue(StringUtils.isBlank(workData.getContributors().get(3).getSequence()));
		assertTrue(workData.getContributors().get(3).getRole().equals("role3"));
		assertTrue(StringUtils.isBlank(workData.getContributors().get(4).getCreditName()));
		assertTrue(workData.getContributors().get(4).getSequence().equals("seq4"));
		assertTrue(workData.getContributors().get(4).getRole().equals("role4"));
	}
}
