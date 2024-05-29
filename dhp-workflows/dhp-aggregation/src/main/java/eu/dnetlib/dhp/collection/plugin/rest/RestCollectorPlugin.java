
package eu.dnetlib.dhp.collection.plugin.rest;

import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;

/**
 * TODO: delegate HTTP requests to the common HttpConnector2 implementation.
 *
 * @author 	js, Andreas Czerniak
 * @date 	2020-04-09
 *
 */
public class RestCollectorPlugin implements CollectorPlugin {

	public static final String RESULT_SIZE_VALUE_DEFAULT = "100";

	private final HttpClientParams clientParams;

	public RestCollectorPlugin(HttpClientParams clientParams) {
		this.clientParams = clientParams;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {
		final String baseUrl = api.getBaseUrl();

		final String resumptionType = api.getParams().get("resumptionType");
		final String resumptionParam = api.getParams().get("resumptionParam");
		final String resumptionXpath = api.getParams().get("resumptionXpath");
		final String resultTotalXpath = api.getParams().get("resultTotalXpath");
		final String resultFormatParam = api.getParams().get("resultFormatParam");
		final String resultFormatValue = api.getParams().get("resultFormatValue");
		final String resultSizeParam = api.getParams().get("resultSizeParam");
		final String queryParams = api.getParams().get("queryParams");
		final String entityXpath = api.getParams().get("entityXpath");
		final String authMethod = api.getParams().get("authMethod");
		final String authToken = api.getParams().get("authToken");
		final String requestHeaderMap = api.getParams().get("requestHeaderMap");
		Gson gson = new Gson();
		Map requestHeaders = gson.fromJson(requestHeaderMap, Map.class);
		final String resultSizeValue = Optional
			.ofNullable(api.getParams().get("resultSizeValue"))
			.filter(StringUtils::isNotBlank)
			.orElse(RESULT_SIZE_VALUE_DEFAULT);

		if (StringUtils.isBlank(baseUrl)) {
			throw new CollectorException("Param 'baseUrl' is null or empty");
		}
		if (StringUtils.isBlank(resumptionType)) {
			throw new CollectorException("Param 'resumptionType' is null or empty");
		}
		if (StringUtils.isBlank(resumptionParam)) {
			throw new CollectorException("Param 'resumptionParam' is null or empty");
		}
		if (StringUtils.isBlank(resultFormatValue)) {
			throw new CollectorException("Param 'resultFormatValue' is null or empty");
		}
		if (StringUtils.isBlank(entityXpath)) {
			throw new CollectorException("Param 'entityXpath' is null or empty");
		}

		final String resultOutputFormat = Optional
			.ofNullable(api.getParams().get("resultOutputFormat"))
			.map(String::toLowerCase)
			.filter(StringUtils::isNotBlank)
			.orElse(resultFormatValue.toLowerCase());

		RestIterator it = new RestIterator(
			getClientParams(),
			baseUrl,
			resumptionType,
			resumptionParam,
			resumptionXpath,
			resultTotalXpath,
			resultFormatParam,
			resultFormatValue,
			resultSizeParam,
			resultSizeValue,
			queryParams,
			entityXpath,
			authMethod,
			authToken,
			resultOutputFormat,
				requestHeaders);

		return StreamSupport
			.stream(
				Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);
	}

	public HttpClientParams getClientParams() {
		return clientParams;
	}
}
