/**
 * 
 */

package eu.dnetlib.dhp.collection.plugin.rest;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

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

	private final String baseUrl = "https://ddh-openapi.worldbank.org/search";
	private final String resumptionType = "discover";
	private final String resumptionParam = "skip";
	private final String entityXpath = "//*[local-name()='data']";
	private final String resumptionXpath = "";
	private final String resultTotalXpath = "//*[local-name()='count']";
	private final String resultFormatParam = "";
	private final String resultFormatValue = "json";
	private final String resultSizeParam = "top";
	private final String resultSizeValue = "10";
	// private String query = "q=%28sources%3ASocArXiv+AND+type%3Apreprint%29";
	private final String query = "";
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
		params.put("requestHeaderMap", "{\"User-Agent\": \"OpenAIRE DEV\"}");

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

	@Disabled
	@Test
	void testUrl() throws IOException {
		String url_s = "https://ddh-openapi.worldbank.org/search?&top=10";
		URL url = new URL(url_s);
		final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("User-Agent", "OpenAIRE");
		Gson gson = new Gson();
		System.out.println("Request header");
		System.out.println(gson.toJson(conn.getHeaderFields()));
		InputStream inputStream = conn.getInputStream();

	}
}
