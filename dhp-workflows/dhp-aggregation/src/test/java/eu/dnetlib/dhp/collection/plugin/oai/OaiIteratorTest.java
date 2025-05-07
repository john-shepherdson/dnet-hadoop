
package eu.dnetlib.dhp.collection.plugin.oai;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

@Disabled
class OaiIteratorTest {

	private static final Logger log = LoggerFactory.getLogger(OaiIteratorTest.class);

	private OaiIterator oaiIterator;

	@BeforeEach
	void setUp() throws Exception {
		final String baseUrl = "https://metadata.openedition.org/oai";
		final String mdFormat = "oai_openaire";
		final HttpConnector2 httpConnector = new HttpConnector2();
		final AggregatorReport report = new AggregatorReport();

		this.oaiIterator = new OaiIterator(baseUrl, mdFormat, null, null, null, httpConnector, report);
	}

	@Test
	void test() {
		while (this.oaiIterator.hasNext()) {
			final String next = this.oaiIterator.next();
			log.info("Next OAI record: {}", next);
		}
		log.info("No more records to fetch.");
	}

}
