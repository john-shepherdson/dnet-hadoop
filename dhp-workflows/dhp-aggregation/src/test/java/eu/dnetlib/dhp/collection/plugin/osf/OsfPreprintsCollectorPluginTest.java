
package eu.dnetlib.dhp.collection.plugin.osf;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.dom4j.DocumentHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.utils.JsonUtils;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class OsfPreprintsCollectorPluginTest {

	private static final Logger log = LoggerFactory.getLogger(OsfPreprintsCollectorPlugin.class);

	private final String baseUrl = "https://api.osf.io/v2/preprints/";

	private final int pageSize = 100;

	private final ApiDescriptor api = new ApiDescriptor();

	private OsfPreprintsCollectorPlugin plugin;

	@BeforeEach
	public void setUp() {
		final HashMap<String, String> params = new HashMap<>();
		params.put("pageSize", "" + this.pageSize);

		this.api.setBaseUrl(this.baseUrl);
		this.api.setParams(params);

		this.plugin = new OsfPreprintsCollectorPlugin(new HttpClientParams());
	}

	@Test
	@Disabled
	void test_one() throws CollectorException {
		this.plugin
			.collect(this.api, new AggregatorReport())
			.limit(1)
			.forEach(log::info);
	}

	@Test
	@Disabled
	void test_limited() throws CollectorException {
		final AtomicInteger i = new AtomicInteger(0);
		final Stream<String> stream = this.plugin.collect(this.api, new AggregatorReport());

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
		final Stream<String> stream = this.plugin.collect(this.api, new AggregatorReport());

		stream.forEach(s -> {
			Assertions.assertTrue(s.length() > 0);
			if ((i.incrementAndGet() % 1000) == 0) {
				log.info("COLLECTED: {}", i.get());
			}

		});

		log.info("TOTAL: {}", i.get());
		Assertions.assertTrue(i.get() > 0);
	}

	@Test
	@Disabled
	void test_authentication_required() {
		final HttpConnector2 connector = new HttpConnector2();

		try {
			final String res = connector
				.getInputSource("https://api.osf.io/v2/preprints/ydtzx/contributors/?format=json");
			System.out.println(res);
			fail();
		} catch (final Throwable e) {

			System.out.println("**** ERROR: " + e.getMessage());

			if ((e instanceof CollectorException) && e.getMessage().contains("401")) {
				System.out.println(" XML: " + DocumentHelper.createDocument().getRootElement().detach());
			}

			assertTrue(e.getMessage().contains("401"));
		}

	}

	@Test
	void testXML() {
		final String xml = JsonUtils.convertToXML("{'next':null}");
		System.out.println(xml);
	}

}
