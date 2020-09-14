
package eu.dnetlib.dhp.provision;

import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.map.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class DropAndCreateESIndex {

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					DropAndCreateESIndex.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/provision/dropAndCreateIndex.json")));
		parser.parseArgument(args);

		final String index = parser.get("index");

		final String cluster = parser.get("cluster");
		final String clusterJson = IOUtils
			.toString(DropAndCreateESIndex.class.getResourceAsStream("/eu/dnetlib/dhp/provision/cluster.json"));

		final Map<String, String> clusterMap = new ObjectMapper().readValue(clusterJson, Map.class);

		final String ip = clusterMap.get(cluster).split(",")[0];

		System.out.println(ip);

		final String url = "http://%s:9200/%s_%s";

		CloseableHttpClient client = HttpClients.createDefault();

		HttpDelete delete = new HttpDelete(String.format(url, ip, index, "object"));

		CloseableHttpResponse response = client.execute(delete);

		System.out.println("deleting Index SUMMARY");
		System.out.println(response.getStatusLine());
		client.close();
		client = HttpClients.createDefault();

		delete = new HttpDelete(String.format(url, ip, index, "scholix"));

		response = client.execute(delete);

		System.out.println("deleting Index SCHOLIX");
		System.out.println(response.getStatusLine());
		client.close();
		client = HttpClients.createDefault();

		final String summaryConf = IOUtils
			.toString(DropAndCreateESIndex.class.getResourceAsStream("/eu/dnetlib/dhp/provision/summary_index.json"));

		final String scholixConf = IOUtils
			.toString(DropAndCreateESIndex.class.getResourceAsStream("/eu/dnetlib/dhp/provision/scholix_index.json"));

		HttpPut put = new HttpPut(String.format(url, ip, index, "object"));

		StringEntity entity = new StringEntity(summaryConf);
		put.setEntity(entity);
		put.setHeader("Accept", "application/json");
		put.setHeader("Content-type", "application/json");

		System.out.println("creating First Index SUMMARY");
		response = client.execute(put);

		client.close();
		client = HttpClients.createDefault();

		System.out.println(response.getStatusLine());

		System.out.println("creating Index SCHOLIX");
		put = new HttpPut(String.format(url, ip, index, "scholix"));

		entity = new StringEntity(scholixConf);
		put.setEntity(entity);
		put.setHeader("Accept", "application/json");
		put.setHeader("Content-type", "application/json");

		response = client.execute(put);
		System.out.println(response.getStatusLine());
		client.close();

	}
}
