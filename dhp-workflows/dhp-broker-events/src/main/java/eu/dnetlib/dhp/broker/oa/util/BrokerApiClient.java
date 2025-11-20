package eu.dnetlib.dhp.broker.oa.util;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.broker.model.Subscription;
import eu.dnetlib.dhp.broker.oa.util.aggregators.stats.DatasourceStats;
import eu.dnetlib.dhp.utils.DHPUtils;

public class BrokerApiClient {

	private interface ResponseMapper<T> {

		T map(HttpResponse res) throws IOException;
	}

	public static Subscription[] listSubscriptions(final String brokerApiBaseUrl) throws Exception {
		final String url = brokerApiBaseUrl + "/api/subscriptions";

		return performHttpGet(url, Subscription[].class, res -> {
			final ObjectMapper mapper = new ObjectMapper();
			final String s = IOUtils.toString(res.getEntity().getContent());
			return mapper
					.readValue(s, mapper.getTypeFactory().constructArrayType(Subscription.class));
		});

	}

	public static String updateStats(final String brokerApiBaseUrl) throws IOException {
		final String url = brokerApiBaseUrl + "/api/openaireBroker/stats/update";
		return performHttpGet(url, String.class, res -> IOUtils.toString(res.getEntity().getContent()));
	}

	public static void updateAlertStats(final String brokerApiBaseUrl, final DatasourceStats stats) throws IOException {
		final String url = brokerApiBaseUrl + "/api/openaire-alerts/stats/update";
		performHttpPostJson(url, stats);
	}

	public static String sendNotifications(final String brokerApiBaseUrl, final long l) throws IOException {
		final String url = brokerApiBaseUrl + "/api/openaireBroker/notifications/send/" + l;
		return performHttpGet(url, String.class, res -> IOUtils.toString(res.getEntity().getContent()));
	}

	public static String sendAlertNotifications(final String brokerApiBaseUrl, final String dsId) throws IOException {
		final String url = brokerApiBaseUrl + "/api/openaire-alerts/notifications/sendNotificationsForDatasource?dsId=" + dsId;
		return performHttpGet(url, String.class, res -> IOUtils.toString(res.getEntity().getContent()));
	}

	private static <T> T performHttpGet(final String url, final Class<T> responseClass, final ResponseMapper<T> mapper) throws IOException {
		final HttpGet req = new HttpGet(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				return mapper.map(response);
			}
		}
	}

	private static void performHttpPostJson(final String url, final Object o)
			throws JsonProcessingException, IOException, ClientProtocolException {

		final HttpPost req = new HttpPost(url);
		req.setHeader("Accept", "application/json");
		req.setHeader("Content-type", "application/json");

		req.setEntity(new StringEntity(DHPUtils.MAPPER.writeValueAsString(o), ContentType.APPLICATION_JSON));

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {

			}
		}
	}

}
