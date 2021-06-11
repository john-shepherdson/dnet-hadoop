
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;

public class OafMapperUtilsTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Test
	public void testDateValidation() {

		assertTrue(GraphCleaningFunctions.doCleanDate("2016-05-07T12:41:19.202Z  ").isPresent());
		assertTrue(GraphCleaningFunctions.doCleanDate("2020-09-10 11:08:52 ").isPresent());
		assertTrue(GraphCleaningFunctions.doCleanDate("  2016-04-05").isPresent());

		assertEquals("2016-04-05", GraphCleaningFunctions.doCleanDate("2016 Apr 05").get());

		assertEquals("2009-05-08", GraphCleaningFunctions.doCleanDate("May 8, 2009 5:57:51 PM").get());
		assertEquals("1970-10-07", GraphCleaningFunctions.doCleanDate("oct 7, 1970").get());
		assertEquals("1970-10-07", GraphCleaningFunctions.doCleanDate("oct 7, '70").get());
		assertEquals("1970-10-07", GraphCleaningFunctions.doCleanDate("oct. 7, 1970").get());
		assertEquals("1970-10-07", GraphCleaningFunctions.doCleanDate("oct. 7, 70").get());
		assertEquals("2006-01-02", GraphCleaningFunctions.doCleanDate("Mon Jan  2 15:04:05 2006").get());
		assertEquals("2006-01-02", GraphCleaningFunctions.doCleanDate("Mon Jan  2 15:04:05 MST 2006").get());
		assertEquals("2006-01-02", GraphCleaningFunctions.doCleanDate("Mon Jan 02 15:04:05 -0700 2006").get());
		assertEquals("2006-01-02", GraphCleaningFunctions.doCleanDate("Monday, 02-Jan-06 15:04:05 MST").get());
		assertEquals("2006-01-02", GraphCleaningFunctions.doCleanDate("Mon, 02 Jan 2006 15:04:05 MST").get());
		assertEquals("2017-07-11", GraphCleaningFunctions.doCleanDate("Tue, 11 Jul 2017 16:28:13 +0200 (CEST)").get());
		assertEquals("2006-01-02", GraphCleaningFunctions.doCleanDate("Mon, 02 Jan 2006 15:04:05 -0700").get());
		assertEquals("2018-01-04", GraphCleaningFunctions.doCleanDate("Thu, 4 Jan 2018 17:53:36 +0000").get());
		assertEquals("2015-08-10", GraphCleaningFunctions.doCleanDate("Mon Aug 10 15:44:11 UTC+0100 2015").get());
		assertEquals(
			"2015-07-03",
			GraphCleaningFunctions.doCleanDate("Fri Jul 03 2015 18:04:07 GMT+0100 (GMT Daylight Time)").get());
		assertEquals("2012-09-17", GraphCleaningFunctions.doCleanDate("September 17, 2012 10:09am").get());
		assertEquals("2012-09-17", GraphCleaningFunctions.doCleanDate("September 17, 2012 at 10:09am PST-08").get());
		assertEquals("2012-09-17", GraphCleaningFunctions.doCleanDate("September 17, 2012, 10:10:09").get());
		assertEquals("1970-10-07", GraphCleaningFunctions.doCleanDate("October 7, 1970").get());
		assertEquals("1970-10-07", GraphCleaningFunctions.doCleanDate("October 7th, 1970").get());
		assertEquals("2006-02-12", GraphCleaningFunctions.doCleanDate("12 Feb 2006, 19:17").get());
		assertEquals("2006-02-12", GraphCleaningFunctions.doCleanDate("12 Feb 2006 19:17").get());
		assertEquals("1970-10-07", GraphCleaningFunctions.doCleanDate("7 oct 70").get());
		assertEquals("1970-10-07", GraphCleaningFunctions.doCleanDate("7 oct 1970").get());
		assertEquals("2013-02-03", GraphCleaningFunctions.doCleanDate("03 February 2013").get());
		assertEquals("2013-07-01", GraphCleaningFunctions.doCleanDate("1 July 2013").get());
		assertEquals("2013-02-03", GraphCleaningFunctions.doCleanDate("2013-Feb-03").get());
		assertEquals("2014-03-31", GraphCleaningFunctions.doCleanDate("3/31/2014").get());
		assertEquals("2014-03-31", GraphCleaningFunctions.doCleanDate("03/31/2014").get());
		assertEquals("1971-08-21", GraphCleaningFunctions.doCleanDate("08/21/71").get());
		assertEquals("1971-01-08", GraphCleaningFunctions.doCleanDate("8/1/71").get());
		assertEquals("2014-08-04", GraphCleaningFunctions.doCleanDate("4/8/2014 22:05").get());
		assertEquals("2014-08-04", GraphCleaningFunctions.doCleanDate("04/08/2014 22:05").get());
		assertEquals("2014-08-04", GraphCleaningFunctions.doCleanDate("4/8/14 22:05").get());
		assertEquals("2014-02-04", GraphCleaningFunctions.doCleanDate("04/2/2014 03:00:51").get());
		assertEquals("1965-08-08", GraphCleaningFunctions.doCleanDate("8/8/1965 12:00:00 AM").get());
		assertEquals("1965-08-08", GraphCleaningFunctions.doCleanDate("8/8/1965 01:00:01 PM").get());
		assertEquals("1965-08-08", GraphCleaningFunctions.doCleanDate("8/8/1965 01:00 PM").get());
		assertEquals("1965-08-08", GraphCleaningFunctions.doCleanDate("8/8/1965 1:00 PM").get());
		assertEquals("1965-08-08", GraphCleaningFunctions.doCleanDate("8/8/1965 12:00 AM").get());
		assertEquals("2014-02-04", GraphCleaningFunctions.doCleanDate("4/02/2014 03:00:51").get());
		assertEquals("2012-03-19", GraphCleaningFunctions.doCleanDate("03/19/2012 10:11:59").get());
		assertEquals("2012-03-19", GraphCleaningFunctions.doCleanDate("03/19/2012 10:11:59.3186369").get());
		assertEquals("2014-03-31", GraphCleaningFunctions.doCleanDate("2014/3/31").get());
		assertEquals("2014-03-31", GraphCleaningFunctions.doCleanDate("2014/03/31").get());
		assertEquals("2014-04-08", GraphCleaningFunctions.doCleanDate("2014/4/8 22:05").get());
		assertEquals("2014-04-08", GraphCleaningFunctions.doCleanDate("2014/04/08 22:05").get());
		assertEquals("2014-04-02", GraphCleaningFunctions.doCleanDate("2014/04/2 03:00:51").get());
		assertEquals("2014-04-02", GraphCleaningFunctions.doCleanDate("2014/4/02 03:00:51").get());
		assertEquals("2012-03-19", GraphCleaningFunctions.doCleanDate("2012/03/19 10:11:59").get());
		assertEquals("2012-03-19", GraphCleaningFunctions.doCleanDate("2012/03/19 10:11:59.3186369").get());
		assertEquals("2014-04-08", GraphCleaningFunctions.doCleanDate("2014年04月08日").get());
		assertEquals("2006-01-02", GraphCleaningFunctions.doCleanDate("2006-01-02T15:04:05+0000").get());
		assertEquals("2009-08-13", GraphCleaningFunctions.doCleanDate("2009-08-12T22:15:09-07:00").get());
		assertEquals("2009-08-12", GraphCleaningFunctions.doCleanDate("2009-08-12T22:15:09").get());
		assertEquals("2009-08-12", GraphCleaningFunctions.doCleanDate("2009-08-12T22:15:09Z").get());
		assertEquals("2014-04-26", GraphCleaningFunctions.doCleanDate("2014-04-26 17:24:37.3186369").get());
		assertEquals("2012-08-03", GraphCleaningFunctions.doCleanDate("2012-08-03 18:31:59.257000000").get());
		assertEquals("2014-04-26", GraphCleaningFunctions.doCleanDate("2014-04-26 17:24:37.123").get());
		assertEquals("2013-04-01", GraphCleaningFunctions.doCleanDate("2013-04-01 22:43").get());
		assertEquals("2013-04-01", GraphCleaningFunctions.doCleanDate("2013-04-01 22:43:22").get());
		assertEquals("2014-12-16", GraphCleaningFunctions.doCleanDate("2014-12-16 06:20:00 UTC").get());
		assertEquals("2014-12-16", GraphCleaningFunctions.doCleanDate("2014-12-16 06:20:00 GMT").get());
		assertEquals("2014-04-26", GraphCleaningFunctions.doCleanDate("2014-04-26 05:24:37 PM").get());
		assertEquals("2014-04-26", GraphCleaningFunctions.doCleanDate("2014-04-26 13:13:43 +0800").get());
		assertEquals("2014-04-26", GraphCleaningFunctions.doCleanDate("2014-04-26 13:13:43 +0800 +08").get());
		assertEquals("2014-04-26", GraphCleaningFunctions.doCleanDate("2014-04-26 13:13:44 +09:00").get());
		assertEquals("2012-08-03", GraphCleaningFunctions.doCleanDate("2012-08-03 18:31:59.257000000 +0000 UTC").get());
		assertEquals("2015-09-30", GraphCleaningFunctions.doCleanDate("2015-09-30 18:48:56.35272715 +0000 UTC").get());
		assertEquals("2015-02-18", GraphCleaningFunctions.doCleanDate("2015-02-18 00:12:00 +0000 GMT").get());
		assertEquals("2015-02-18", GraphCleaningFunctions.doCleanDate("2015-02-18 00:12:00 +0000 UTC").get());
		assertEquals(
			"2015-02-08", GraphCleaningFunctions.doCleanDate("2015-02-08 03:02:00 +0300 MSK m=+0.000000001").get());
		assertEquals(
			"2015-02-08", GraphCleaningFunctions.doCleanDate("2015-02-08 03:02:00.001 +0300 MSK m=+0.000000001").get());
		assertEquals("2017-07-19", GraphCleaningFunctions.doCleanDate("2017-07-19 03:21:51+00:00").get());
		assertEquals("2014-04-26", GraphCleaningFunctions.doCleanDate("2014-04-26").get());
		assertEquals("2014-04-01", GraphCleaningFunctions.doCleanDate("2014-04").get());
		assertEquals("2014-01-01", GraphCleaningFunctions.doCleanDate("2014").get());
		assertEquals("2014-05-11", GraphCleaningFunctions.doCleanDate("2014-05-11 08:20:13,787").get());
		assertEquals("2014-03-31", GraphCleaningFunctions.doCleanDate("3.31.2014").get());
		assertEquals("2014-03-31", GraphCleaningFunctions.doCleanDate("03.31.2014").get());
		assertEquals("1971-08-21", GraphCleaningFunctions.doCleanDate("08.21.71").get());
		assertEquals("2014-03-01", GraphCleaningFunctions.doCleanDate("2014.03").get());
		assertEquals("2014-03-30", GraphCleaningFunctions.doCleanDate("2014.03.30").get());
		assertEquals("2014-06-01", GraphCleaningFunctions.doCleanDate("20140601").get());
		assertEquals("2014-07-22", GraphCleaningFunctions.doCleanDate("20140722105203").get());
		assertEquals("2012-03-19", GraphCleaningFunctions.doCleanDate("1332151919").get());
		assertEquals("2013-11-12", GraphCleaningFunctions.doCleanDate("1384216367189").get());
		assertEquals("2013-11-12", GraphCleaningFunctions.doCleanDate("1384216367111222").get());
		assertEquals("2013-11-12", GraphCleaningFunctions.doCleanDate("1384216367111222333").get());

	}

	@Test
	public void testMergePubs() throws IOException {
		Publication p1 = read("publication_1.json", Publication.class);
		Publication p2 = read("publication_2.json", Publication.class);
		Dataset d1 = read("dataset_1.json", Dataset.class);
		Dataset d2 = read("dataset_2.json", Dataset.class);

		assertEquals(p1.getCollectedfrom().size(), 1);
		assertEquals(p1.getCollectedfrom().get(0).getKey(), ModelConstants.CROSSREF_ID);
		assertEquals(d2.getCollectedfrom().size(), 1);
		assertFalse(cfId(d2.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));

		assertTrue(
			OafMapperUtils
				.mergeResults(p1, d2)
				.getResulttype()
				.getClassid()
				.equals(ModelConstants.PUBLICATION_RESULTTYPE_CLASSID));

		assertEquals(p2.getCollectedfrom().size(), 1);
		assertFalse(cfId(p2.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));
		assertEquals(d1.getCollectedfrom().size(), 1);
		assertTrue(cfId(d1.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));

		assertTrue(
			OafMapperUtils
				.mergeResults(p2, d1)
				.getResulttype()
				.getClassid()
				.equals(ModelConstants.DATASET_RESULTTYPE_CLASSID));
	}

	protected HashSet<String> cfId(List<KeyValue> collectedfrom) {
		return collectedfrom.stream().map(c -> c.getKey()).collect(Collectors.toCollection(HashSet::new));
	}

	protected <T extends Result> T read(String filename, Class<T> clazz) throws IOException {
		final String json = IOUtils.toString(getClass().getResourceAsStream(filename));
		return OBJECT_MAPPER.readValue(json, clazz);
	}

}
