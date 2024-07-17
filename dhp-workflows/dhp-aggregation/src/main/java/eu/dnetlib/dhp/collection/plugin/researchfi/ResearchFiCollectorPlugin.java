
package eu.dnetlib.dhp.collection.plugin.researchfi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class ResearchFiCollectorPlugin implements CollectorPlugin {

	private static final Logger log = LoggerFactory.getLogger(ResearchFiCollectorPlugin.class);

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report)
		throws CollectorException {

		final String authUrl = api.getParams().get("auth_url");
		final String clientId = api.getParams().get("auth_client_id");
		final String clientSecret = api.getParams().get("auth_client_secret");

		final String authToken = authenticate(authUrl, clientId, clientSecret);

		final Iterator<String> iter = new ResearchFiIterator(api.getBaseUrl(), authToken);

		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
	}

	private String authenticate(final String authUrl, final String clientId, final String clientSecret)
		throws CollectorException {
		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			final HttpPost req = new HttpPost(authUrl);
			final List<NameValuePair> params = new ArrayList<>();
			params.add(new BasicNameValuePair("grant_type", "client_credentials"));
			params.add(new BasicNameValuePair("client_id", clientId));
			params.add(new BasicNameValuePair("client_secret", clientSecret));

			req.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

			try (final CloseableHttpResponse response = client.execute(req)) {
				final String content = IOUtils.toString(response.getEntity().getContent());
				final JSONObject obj = new JSONObject(content);
				final String token = obj.getString("access_token");
				if (StringUtils.isNotBlank(token)) {
					return token;
				}
			}
		} catch (final Throwable e) {
			log.warn("Error obtaining access token", e);
			throw new CollectorException("Error obtaining access token", e);
		}
		throw new CollectorException("Access token is missing");

	}

}
