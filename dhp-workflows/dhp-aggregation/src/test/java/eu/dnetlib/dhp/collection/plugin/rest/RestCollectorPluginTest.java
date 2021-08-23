/**
 * 
 */

package eu.dnetlib.dhp.collection.plugin.rest;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;

/**
 * @author js, Andreas Czerniak
 *
 */
class RestCollectorPluginTest {

	private static final Logger log = LoggerFactory.getLogger(RestCollectorPluginTest.class);

	private final String baseUrl = "https://share.osf.io/api/v2/search/creativeworks/_search";
	private final String resumptionType = "count";
	private final String resumptionParam = "from";
	private final String entityXpath = "//hits/hits";
	private final String resumptionXpath = "//hits";
	private final String resultTotalXpath = "//hits/total";
	private final String resultFormatParam = "format";
	private final String resultFormatValue = "json";
	private final String resultSizeParam = "size";
	private final String resultSizeValue = "10";
	// private String query = "q=%28sources%3ASocArXiv+AND+type%3Apreprint%29";
	private final String query = "q=%28sources%3AengrXiv+AND+type%3Apreprint%29";
	// private String query = "=(sources:engrXiv AND type:preprint)";

	private final String protocolDescriptor = "rest_json2xml";
	private final ApiDescriptor api = new ApiDescriptor();
	private RestCollectorPlugin rcp;

	@BeforeEach
	public void setUp() {
		HashMap<String, String> params = new HashMap<>();
		params.put("resumptionType", resumptionType);
		params.put("resumptionParam", resumptionParam);
		params.put("resumptionXpath", resumptionXpath);
		params.put("resultTotalXpath", resultTotalXpath);
		params.put("resultFormatParam", resultFormatParam);
		params.put("resultFormatValue", resultFormatValue);
		params.put("resultSizeParam", resultSizeParam);
		params.put("resultSizeValue", resultSizeValue);
		params.put("queryParams", query);
		params.put("entityXpath", entityXpath);

		api.setBaseUrl(baseUrl);
		api.setParams(params);

		rcp = new RestCollectorPlugin(new HttpClientParams());
	}

	@Disabled
	@Test
	void test() throws CollectorException {
		AtomicInteger i = new AtomicInteger(0);
		final Stream<String> stream = rcp.collect(api, new AggregatorReport());

		stream.limit(200).forEach(s -> {
			Assertions.assertTrue(s.length() > 0);
			i.incrementAndGet();
			log.info(s);
		});

		log.info("{}", i.intValue());
		Assertions.assertTrue(i.intValue() > 0);
	}
}
