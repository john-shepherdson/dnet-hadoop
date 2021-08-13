
package eu.dnetlib.dhp.sx.provision;

import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class DropAndCreateESIndex {

	private static final Logger log = LoggerFactory.getLogger(DropAndCreateESIndex.class);
	public static final String STATUS_CODE_TEXT = "status code: {}";
	public static final String APPLICATION_JSON = "application/json";

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							DropAndCreateESIndex.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/sx/provision/dropAndCreateIndex.json"))));
		parser.parseArgument(args);

		final String index = parser.get("index");

		final String cluster = parser.get("cluster");
		final String clusterJson = IOUtils
			.toString(
				Objects
					.requireNonNull(
						DropAndCreateESIndex.class.getResourceAsStream("/eu/dnetlib/dhp/sx/provision/cluster.json")));

		@SuppressWarnings("unchecked")
		Map<String, String> clusterMap = new ObjectMapper().readValue(clusterJson, Map.class);

		final String ip = clusterMap.get(cluster).split(",")[0];

		final String url = "http://%s:9200/%s_%s";

		try (CloseableHttpClient client = HttpClients.createDefault()) {

			HttpDelete delete = new HttpDelete(String.format(url, ip, index, "object"));

			CloseableHttpResponse response = client.execute(delete);

			log.info("deleting Index SUMMARY");
			log.info(STATUS_CODE_TEXT, response.getStatusLine());
		}

		try (CloseableHttpClient client = HttpClients.createDefault()) {

			HttpDelete delete = new HttpDelete(String.format(url, ip, index, "scholix"));

			CloseableHttpResponse response = client.execute(delete);

			log.info("deleting Index SCHOLIX");
			log.info(STATUS_CODE_TEXT, response.getStatusLine());
		}

		log.info("Sleeping 60 seconds to avoid to lost the creation of index request");
		Thread.sleep(60000);

		try (CloseableHttpClient client = HttpClients.createDefault()) {

			final String summaryConf = IOUtils
				.toString(
					Objects
						.requireNonNull(
							DropAndCreateESIndex.class
								.getResourceAsStream("/eu/dnetlib/dhp/sx/provision/summary_index.json")));

			HttpPut put = new HttpPut(String.format(url, ip, index, "object"));

			StringEntity entity = new StringEntity(summaryConf);
			put.setEntity(entity);
			put.setHeader("Accept", APPLICATION_JSON);
			put.setHeader("Content-type", APPLICATION_JSON);

			log.info("creating First Index SUMMARY");
			CloseableHttpResponse response = client.execute(put);
			log.info(STATUS_CODE_TEXT, response.getStatusLine());

		}
		try (CloseableHttpClient client = HttpClients.createDefault()) {

			final String scholixConf = IOUtils
				.toString(
					Objects
						.requireNonNull(
							DropAndCreateESIndex.class
								.getResourceAsStream("/eu/dnetlib/dhp/sx/provision/scholix_index.json")));

			log.info("creating Index SCHOLIX");
			final HttpPut put = new HttpPut(String.format(url, ip, index, "scholix"));

			final StringEntity entity = new StringEntity(scholixConf);
			put.setEntity(entity);
			put.setHeader("Accept", APPLICATION_JSON);
			put.setHeader("Content-type", APPLICATION_JSON);

			final CloseableHttpResponse response = client.execute(put);
			log.info(STATUS_CODE_TEXT, response.getStatusLine());
		}

	}
}
