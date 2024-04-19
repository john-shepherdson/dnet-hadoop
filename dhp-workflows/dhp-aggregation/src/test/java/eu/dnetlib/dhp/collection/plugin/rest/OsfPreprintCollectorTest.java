
package eu.dnetlib.dhp.collection.plugin.rest;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
	private final String resumptionType = "page";
	private final String resumptionXpath = "/*/*[local-name()='links']/*[local-name()='next']";

	private final String resultSizeParam = "";
	private final String resultSizeValue = "";

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
	void test() throws CollectorException {
		final AtomicInteger i = new AtomicInteger(0);
		final Stream<String> stream = this.rcp.collect(this.api, new AggregatorReport());

		stream.limit(200).forEach(s -> {
			Assertions.assertTrue(s.length() > 0);
			i.incrementAndGet();
			log.info(s);
		});

		log.info("{}", i.intValue());
		Assertions.assertTrue(i.intValue() > 0);
	}
}
