
package eu.dnetlib.dhp.collection.plugin.rest;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;

public class OsfPreprintCollectorTest {

	private static final Logger log = LoggerFactory.getLogger(OsfPreprintCollectorTest.class);

	private final String baseUrl = "https://api.osf.io/v2/preprints/";

	// private final String requestHeaderMap = "";
	// private final String authMethod = "";
	// private final String authToken = "";
	// private final String resultOutputFormat = "";

	private final String queryParams = "filter:is_published:d=true";

	private final String entityXpath = "/*/*[local-name()='data']";

	private final String resultTotalXpath = "/*/*[local-name()='links']/*[local-name()='meta']/*[local-name()='total']";

	private final String resumptionParam = "page";
	private final String resumptionType = "scan";
	private final String resumptionXpath = "substring-before(substring-after(/*/*[local-name()='links']/*[local-name()='next'], 'page='), '&')";

	private final String resultSizeParam = "page[size]";
	private final String resultSizeValue = "100";

	private final String resultFormatParam = "format";
	private final String resultFormatValue = "json";

	private final ApiDescriptor api = new ApiDescriptor();
	private RestCollectorPlugin rcp;

	@BeforeEach
	public void setUp() {
		final HashMap<String, String> params = new HashMap<>();
		params.put("resumptionType", this.resumptionType);
		params.put("resumptionParam", this.resumptionParam);
		params.put("resumptionXpath", this.resumptionXpath);
		params.put("resultTotalXpath", this.resultTotalXpath);
		params.put("resultFormatParam", this.resultFormatParam);
		params.put("resultFormatValue", this.resultFormatValue);
		params.put("resultSizeParam", this.resultSizeParam);
		params.put("resultSizeValue", this.resultSizeValue);
		params.put("queryParams", this.queryParams);
		params.put("entityXpath", this.entityXpath);

		this.api.setBaseUrl(this.baseUrl);
		this.api.setParams(params);

		this.rcp = new RestCollectorPlugin(new HttpClientParams());
	}

	@Test
	@Disabled
	void test_limited() throws CollectorException {
		final AtomicInteger i = new AtomicInteger(0);
		final Stream<String> stream = this.rcp.collect(this.api, new AggregatorReport());

		stream.limit(2000).forEach(s -> {
			Assertions.assertTrue(s.length() > 0);
			i.incrementAndGet();
			log.info(s);
		});

		log.info("{}", i.intValue());
		Assertions.assertTrue(i.intValue() > 0);
	}

	@Test
	@Disabled
	void test_all() throws CollectorException {
		final AtomicLong i = new AtomicLong(0);
		final Stream<String> stream = this.rcp.collect(this.api, new AggregatorReport());

		stream.forEach(s -> {
			Assertions.assertTrue(s.length() > 0);
			if ((i.incrementAndGet() % 1000) == 0) {
				log.info("COLLECTED: {}", i.get());
			}

		});

		log.info("TOTAL: {}", i.get());
		Assertions.assertTrue(i.get() > 0);
	}

}
