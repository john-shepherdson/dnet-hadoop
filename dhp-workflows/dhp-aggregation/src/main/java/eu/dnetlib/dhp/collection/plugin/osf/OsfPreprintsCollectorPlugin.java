
package eu.dnetlib.dhp.collection.plugin.osf;

import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;

public class OsfPreprintsCollectorPlugin implements CollectorPlugin {

	public static final int PAGE_SIZE_VALUE_DEFAULT = 100;

	private final HttpClientParams clientParams;

	public OsfPreprintsCollectorPlugin(final HttpClientParams clientParams) {
		this.clientParams = clientParams;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {
		final String baseUrl = api.getBaseUrl();

		final int pageSize = Optional
			.ofNullable(api.getParams().get("pageSize"))
			.filter(StringUtils::isNotBlank)
			.map(s -> NumberUtils.toInt(s, PAGE_SIZE_VALUE_DEFAULT))
			.orElse(PAGE_SIZE_VALUE_DEFAULT);

		if (StringUtils.isBlank(baseUrl)) {
			throw new CollectorException("Param 'baseUrl' is null or empty");
		}

		final OsfPreprintsIterator it = new OsfPreprintsIterator(baseUrl, pageSize, getClientParams());

		return StreamSupport
			.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);
	}

	public HttpClientParams getClientParams() {
		return this.clientParams;
	}
}
