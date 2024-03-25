
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;
import me.xuender.unidecode.Unidecode;

class OafMapperUtilsTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Test
	public void testUnidecode() {

		assertEquals("Liu Ben Mu hiruzuSen tawa", Unidecode.decode("六本木ヒルズ森タワ"));
		assertEquals("Nan Wu A Mi Tuo Fo", Unidecode.decode("南无阿弥陀佛"));
		assertEquals("Yi Tiao Hui Zou Lu De Yu", Unidecode.decode("一条会走路的鱼"));
		assertEquals("amidaniyorai", Unidecode.decode("あみだにょらい"));
		assertEquals("T`owrk`iayi", Unidecode.decode("Թուրքիայի"));
		assertEquals("Obzor tematiki", Unidecode.decode("Обзор тематики"));
		assertEquals("GERMANSKIE IaZYKI", Unidecode.decode("ГЕРМАНСКИЕ ЯЗЫКИ"));
		assertEquals("Diereunese tes ikanopoieses", Unidecode.decode("Διερεύνηση της ικανοποίησης"));
		assertEquals("lqDy l'wly@", Unidecode.decode("القضايا الأولية"));
		assertEquals("abc def ghi", Unidecode.decode("abc def ghi"));
	}

	@Test
	void testDateValidation() {

		assertNotNull(GraphCleaningFunctions.cleanDate("2016-05-07T12:41:19.202Z  "));
		assertNotNull(GraphCleaningFunctions.cleanDate("2020-09-10 11:08:52 "));
		assertNotNull(GraphCleaningFunctions.cleanDate("  2016-04-05"));

		assertEquals("2016-04-05", GraphCleaningFunctions.cleanDate("2016 Apr 05"));

		assertEquals("2009-05-08", GraphCleaningFunctions.cleanDate("May 8, 2009 5:57:51 PM"));
		assertEquals("1970-10-07", GraphCleaningFunctions.cleanDate("oct 7, 1970"));
		assertEquals("1970-10-07", GraphCleaningFunctions.cleanDate("oct 7, '70"));
		assertEquals("1970-10-07", GraphCleaningFunctions.cleanDate("oct. 7, 1970"));
		assertEquals("1970-10-07", GraphCleaningFunctions.cleanDate("oct. 7, 70"));
		assertEquals("2006-01-02", GraphCleaningFunctions.cleanDate("Mon Jan  2 15:04:05 2006"));
		assertEquals("2006-01-02", GraphCleaningFunctions.cleanDate("Mon Jan  2 15:04:05 MST 2006"));
		assertEquals("2006-01-02", GraphCleaningFunctions.cleanDate("Mon Jan 02 15:04:05 -0700 2006"));
		assertEquals("2006-01-02", GraphCleaningFunctions.cleanDate("Monday, 02-Jan-06 15:04:05 MST"));
		assertEquals("2006-01-02", GraphCleaningFunctions.cleanDate("Mon, 02 Jan 2006 15:04:05 MST"));
		assertEquals("2017-07-11", GraphCleaningFunctions.cleanDate("Tue, 11 Jul 2017 16:28:13 +0200 (CEST)"));
		assertEquals("2006-01-02", GraphCleaningFunctions.cleanDate("Mon, 02 Jan 2006 15:04:05 -0700"));
		assertEquals("2018-01-04", GraphCleaningFunctions.cleanDate("Thu, 4 Jan 2018 17:53:36 +0000"));
		assertEquals("2015-08-10", GraphCleaningFunctions.cleanDate("Mon Aug 10 15:44:11 UTC+0100 2015"));
		assertEquals(
			"2015-07-03",
			GraphCleaningFunctions.cleanDate("Fri Jul 03 2015 18:04:07 GMT+0100 (GMT Daylight Time)"));
		assertEquals("2012-09-17", GraphCleaningFunctions.cleanDate("September 17, 2012 10:09am"));
		assertEquals("2012-09-17", GraphCleaningFunctions.cleanDate("September 17, 2012 at 10:09am PST-08"));
		assertEquals("2012-09-17", GraphCleaningFunctions.cleanDate("September 17, 2012, 10:10:09"));
		assertEquals("1970-10-07", GraphCleaningFunctions.cleanDate("October 7, 1970"));
		assertEquals("1970-10-07", GraphCleaningFunctions.cleanDate("October 7th, 1970"));
		assertEquals("2006-02-12", GraphCleaningFunctions.cleanDate("12 Feb 2006, 19:17"));
		assertEquals("2006-02-12", GraphCleaningFunctions.cleanDate("12 Feb 2006 19:17"));
		assertEquals("1970-10-07", GraphCleaningFunctions.cleanDate("7 oct 70"));
		assertEquals("1970-10-07", GraphCleaningFunctions.cleanDate("7 oct 1970"));
		assertEquals("2013-02-03", GraphCleaningFunctions.cleanDate("03 February 2013"));
		assertEquals("2013-07-01", GraphCleaningFunctions.cleanDate("1 July 2013"));
		assertEquals("2013-02-03", GraphCleaningFunctions.cleanDate("2013-Feb-03"));
		assertEquals("2014-03-31", GraphCleaningFunctions.cleanDate("3/31/2014"));
		assertEquals("2014-03-31", GraphCleaningFunctions.cleanDate("03/31/2014"));
		assertEquals("1971-08-21", GraphCleaningFunctions.cleanDate("08/21/71"));
		assertEquals("1971-01-08", GraphCleaningFunctions.cleanDate("8/1/71"));
		assertEquals("2014-08-04", GraphCleaningFunctions.cleanDate("4/8/2014 22:05"));
		assertEquals("2014-08-04", GraphCleaningFunctions.cleanDate("04/08/2014 22:05"));
		assertEquals("2014-08-04", GraphCleaningFunctions.cleanDate("4/8/14 22:05"));
		assertEquals("2014-02-04", GraphCleaningFunctions.cleanDate("04/2/2014 03:00:51"));
		assertEquals("1965-08-08", GraphCleaningFunctions.cleanDate("8/8/1965 12:00:00 AM"));
		assertEquals("1965-08-08", GraphCleaningFunctions.cleanDate("8/8/1965 01:00:01 PM"));
		assertEquals("1965-08-08", GraphCleaningFunctions.cleanDate("8/8/1965 01:00 PM"));
		assertEquals("1965-08-08", GraphCleaningFunctions.cleanDate("8/8/1965 1:00 PM"));
		assertEquals("1965-08-08", GraphCleaningFunctions.cleanDate("8/8/1965 12:00 AM"));
		assertEquals("2014-02-04", GraphCleaningFunctions.cleanDate("4/02/2014 03:00:51"));
		assertEquals("2012-03-19", GraphCleaningFunctions.cleanDate("03/19/2012 10:11:59"));
		assertEquals("2012-03-19", GraphCleaningFunctions.cleanDate("03/19/2012 10:11:59.3186369"));
		assertEquals("2014-03-31", GraphCleaningFunctions.cleanDate("2014/3/31"));
		assertEquals("2014-03-31", GraphCleaningFunctions.cleanDate("2014/03/31"));
		assertEquals("2014-04-08", GraphCleaningFunctions.cleanDate("2014/4/8 22:05"));
		assertEquals("2014-04-08", GraphCleaningFunctions.cleanDate("2014/04/08 22:05"));
		assertEquals("2014-04-02", GraphCleaningFunctions.cleanDate("2014/04/2 03:00:51"));
		assertEquals("2014-04-02", GraphCleaningFunctions.cleanDate("2014/4/02 03:00:51"));
		assertEquals("2012-03-19", GraphCleaningFunctions.cleanDate("2012/03/19 10:11:59"));
		assertEquals("2012-03-19", GraphCleaningFunctions.cleanDate("2012/03/19 10:11:59.3186369"));
		assertEquals("2014-04-08", GraphCleaningFunctions.cleanDate("2014年04月08日"));
		assertEquals("2006-01-02", GraphCleaningFunctions.cleanDate("2006-01-02T15:04:05+0000"));
		assertEquals("2009-08-13", GraphCleaningFunctions.cleanDate("2009-08-12T22:15:09-07:00"));
		assertEquals("2009-08-12", GraphCleaningFunctions.cleanDate("2009-08-12T22:15:09"));
		assertEquals("2014-04-26", GraphCleaningFunctions.cleanDate("2014-04-26 17:24:37.3186369"));
		assertEquals("2012-08-03", GraphCleaningFunctions.cleanDate("2012-08-03 18:31:59.257000000"));
		assertEquals("2014-04-26", GraphCleaningFunctions.cleanDate("2014-04-26 17:24:37.123"));
		assertEquals("2013-04-01", GraphCleaningFunctions.cleanDate("2013-04-01 22:43"));
		assertEquals("2013-04-01", GraphCleaningFunctions.cleanDate("2013-04-01 22:43:22"));
		assertEquals("2014-12-16", GraphCleaningFunctions.cleanDate("2014-12-16 06:20:00 UTC"));
		assertEquals("2014-12-16", GraphCleaningFunctions.cleanDate("2014-12-16 06:20:00 GMT"));
		assertEquals("2014-04-26", GraphCleaningFunctions.cleanDate("2014-04-26 05:24:37 PM"));
		assertEquals("2014-04-26", GraphCleaningFunctions.cleanDate("2014-04-26 13:13:43 +0800"));
		assertEquals("2014-04-26", GraphCleaningFunctions.cleanDate("2014-04-26 13:13:43 +0800 +08"));
		assertEquals("2014-04-26", GraphCleaningFunctions.cleanDate("2014-04-26 13:13:44 +09:00"));
		assertEquals("2012-08-03", GraphCleaningFunctions.cleanDate("2012-08-03 18:31:59.257000000 +0000 UTC"));
		assertEquals("2015-09-30", GraphCleaningFunctions.cleanDate("2015-09-30 18:48:56.35272715 +0000 UTC"));
		assertEquals("2015-02-18", GraphCleaningFunctions.cleanDate("2015-02-18 00:12:00 +0000 GMT"));
		assertEquals("2015-02-18", GraphCleaningFunctions.cleanDate("2015-02-18 00:12:00 +0000 UTC"));
		assertEquals(
			"2015-02-08", GraphCleaningFunctions.cleanDate("2015-02-08 03:02:00 +0300 MSK m=+0.000000001"));
		assertEquals(
			"2015-02-08", GraphCleaningFunctions.cleanDate("2015-02-08 03:02:00.001 +0300 MSK m=+0.000000001"));
		assertEquals("2017-07-19", GraphCleaningFunctions.cleanDate("2017-07-19 03:21:51+00:00"));
		assertEquals("2014-04-26", GraphCleaningFunctions.cleanDate("2014-04-26"));
		assertEquals("2014-04-01", GraphCleaningFunctions.cleanDate("2014-04"));
		assertEquals("2014-01-01", GraphCleaningFunctions.cleanDate("2014"));
		assertEquals("2014-05-11", GraphCleaningFunctions.cleanDate("2014-05-11 08:20:13,787"));
		assertEquals("2014-03-31", GraphCleaningFunctions.cleanDate("3.31.2014"));
		assertEquals("2014-03-31", GraphCleaningFunctions.cleanDate("03.31.2014"));
		assertEquals("1971-08-21", GraphCleaningFunctions.cleanDate("08.21.71"));
		assertEquals("2014-03-01", GraphCleaningFunctions.cleanDate("2014.03"));
		assertEquals("2014-03-30", GraphCleaningFunctions.cleanDate("2014.03.30"));
		assertEquals("2014-06-01", GraphCleaningFunctions.cleanDate("20140601"));
		assertEquals("2014-07-22", GraphCleaningFunctions.cleanDate("20140722105203"));
		assertEquals("2012-03-19", GraphCleaningFunctions.cleanDate("1332151919"));
		assertEquals("2013-11-12", GraphCleaningFunctions.cleanDate("1384216367189"));
		assertEquals("2013-11-12", GraphCleaningFunctions.cleanDate("1384216367111222"));
		assertEquals("2013-11-12", GraphCleaningFunctions.cleanDate("1384216367111222333"));

	}

	@Test
	void testDate() {
		final String date = GraphCleaningFunctions.cleanDate("23-FEB-1998");
		assertNotNull(date);
		assertEquals("1998-02-23", date);
	}

	@Test
	void testMergePubs() throws IOException {
		Publication p1 = read("publication_1.json", Publication.class);
		Publication p2 = read("publication_2.json", Publication.class);
		Dataset d1 = read("dataset_1.json", Dataset.class);
		Dataset d2 = read("dataset_2.json", Dataset.class);

		assertEquals(1, p1.getCollectedfrom().size());
		assertEquals(ModelConstants.CROSSREF_ID, p1.getCollectedfrom().get(0).getKey());
		assertEquals(1, d2.getCollectedfrom().size());
		assertFalse(cfId(d2.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));

		assertEquals(
			ModelConstants.PUBLICATION_RESULTTYPE_CLASSID,
			MergeUtils
				.mergeResult(p1, d2)
				.getResulttype()
				.getClassid());

		assertEquals(1, p2.getCollectedfrom().size());
		assertFalse(cfId(p2.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));
		assertEquals(1, d1.getCollectedfrom().size());
		assertTrue(cfId(d1.getCollectedfrom()).contains(ModelConstants.CROSSREF_ID));

		assertEquals(
			ModelConstants.DATASET_RESULTTYPE_CLASSID,
			MergeUtils
				.mergeResult(p2, d1)
				.getResulttype()
				.getClassid());
	}

	@Test
	void testDelegatedAuthority() throws IOException {
		Dataset d1 = read("dataset_2.json", Dataset.class);
		Dataset d2 = read("dataset_delegated.json", Dataset.class);

		assertEquals(1, d2.getCollectedfrom().size());
		assertTrue(cfId(d2.getCollectedfrom()).contains(ModelConstants.ZENODO_OD_ID));

		Result res = MergeUtils.mergeResult(d1, d2);

		assertEquals(d2, res);

		System.out.println(OBJECT_MAPPER.writeValueAsString(res));

	}

	protected HashSet<String> cfId(List<KeyValue> collectedfrom) {
		return collectedfrom.stream().map(KeyValue::getKey).collect(Collectors.toCollection(HashSet::new));
	}

	protected <T extends Result> T read(String filename, Class<T> clazz) throws IOException {
		final String json = IOUtils.toString(getClass().getResourceAsStream(filename));
		return OBJECT_MAPPER.readValue(json, clazz);
	}

}
