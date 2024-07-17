package eu.dnetlib.dhp.collection.plugin.researchfi;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class ResearchFiCollectorPluginTest {

	private final ResearchFiCollectorPlugin plugin = new ResearchFiCollectorPlugin();

	@Test
	@Disabled
	void testCollect() throws CollectorException {
		final ApiDescriptor api = new ApiDescriptor();
		api.setBaseUrl("https://research.fi/api/rest/v1/funding-decisions?FunderName=AKA&FundingStartYearFrom=2022");
		api.setProtocol("research_fi");
		api.getParams().put("auth_url", "https://researchfi-auth.2.rahtiapp.fi/realms/publicapi/protocol/openid-connect/token");
		api.getParams().put("auth_client_id", "");
		api.getParams().put("auth_client_secret", "");

		final AtomicLong count = new AtomicLong(0);
		final Set<String> ids = new HashSet<>();

		this.plugin.collect(api, new AggregatorReport()).forEach(s -> {

			if (count.getAndIncrement() == 0) {
				System.out.println("First: " + s);
			}

			try {
				final String id = DocumentHelper.parseText(s).valueOf("/recordWrap/funderProjectNumber");
				if (ids.contains(id)) {
					System.out.println("Id already present: " + id);
				}
				ids.add(id);
			} catch (final DocumentException e) {
				throw new RuntimeException(e);
			}
		});

		System.out.println("Total records: " + count);
		System.out.println("Total identifiers: " + ids.size());

	}

}
