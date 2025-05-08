package eu.dnetlib.dhp.collection.plugin.omicsdi;

import java.util.Arrays;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class OmicsDICollectorPlugin implements CollectorPlugin {

	private static final int PAGE_SIZE_VALUE_DEFAULT = 1000;

	private static final Logger log = LoggerFactory.getLogger(OmicsDICollectorPlugin.class);

	private final HttpClientParams clientParams;

	public OmicsDICollectorPlugin(final HttpClientParams clientParams) {
		this.clientParams = clientParams;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {

		// It should be https://www.omicsdi.org/ws

		final String baseUrl = api.getBaseUrl();

		final int pageSize = Optional
				.ofNullable(api.getParams().get("pageSize"))
				.filter(StringUtils::isNotBlank)
				.map(s -> NumberUtils.toInt(s, PAGE_SIZE_VALUE_DEFAULT))
				.orElse(PAGE_SIZE_VALUE_DEFAULT);

		try {
			final HttpConnector2 connector = new HttpConnector2(this.clientParams);
			final OmicsDIDatabase[] dbs = new ObjectMapper().readValue(connector.getInputSource(baseUrl + "/database/all"), OmicsDIDatabase[].class);

			return Arrays.stream(dbs)
					.map(OmicsDIDatabase::getRepository)
					.filter(StringUtils::isNotBlank)
					.map(repo -> new OmicsDIDatabaseIterator(baseUrl, repo, pageSize, this.clientParams))
					.map(it -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false))
					.flatMap(s -> s);
		} catch (final Throwable e) {
			log.error("Collection failed", e);
			throw new CollectorException("Collection failed", e);
		}
	}

}
