
package eu.dnetlib.sx;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;

public class ESFeeder implements Closeable {

	private ElasticsearchClient esClient;
	private static final Pattern scholixIDPattern = Pattern.compile("\"identifier\":\"((\\d|\\w)*)\"");
	private static final Pattern summaryIDPattern = Pattern.compile("\"dnetIdentifier\":\"((\\d|\\w)*)\"");
	private static final Pattern summaryTypePattern = Pattern.compile("\"objectType\":\"((\\d|\\w)*)\"");
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public ESFeeder(String url) {
		final RestClient client = RestClient
			.builder(
				HttpHost.create(url))
				.setRequestConfigCallback(conf -> conf
					.setConnectTimeout(60000)
					.setSocketTimeout(60000))
			.build();

		final ElasticsearchTransport transport = new RestClientTransport(
			client, new JacksonJsonpMapper());

		this.esClient = new ElasticsearchClient(transport);

	}

	public String search(String text, Pattern pattern) {

		final Matcher matcher = pattern.matcher(text);

		if (matcher.find()) {
			return matcher.group(1);
		}
		return null;
	}

	private BulkOperation createBulkOperation(final String rawJson, String index) throws JsonProcessingException {

		if (index.equalsIgnoreCase("scholix")) {

			final String id = search(rawJson, scholixIDPattern);

			return new BulkOperation.Builder()
				.index(
					i -> i
						.index("scholix")
						.id(id)
						.document(BinaryData.of(rawJson.getBytes(StandardCharsets.UTF_8), "application/json")))
				.build();
		}
		if (index.equalsIgnoreCase("summary")) {
			final String dnetIdentifier = search(rawJson, summaryIDPattern);
			final String objectType = search(rawJson, summaryTypePattern);

			Map<String, String> d = new HashMap<>();
			d.put("objectType", objectType);
			d.put("body", rawJson);
			String data = MAPPER.writeValueAsString(d);

			BulkOperation result;
			result = new BulkOperation.Builder()
				.index(
					i -> i
						.index("summary")
						.id(dnetIdentifier)
						.document(BinaryData.of(data.getBytes(StandardCharsets.UTF_8), "application/json")))
				.build();

			return result;
		}
		return null;
	}

	private void tryBulk(ElasticsearchClient client, BulkRequest bulkRequest, int numberOfTry) {

		for (int i = 0; i < numberOfTry; i++) {
			try {
				BulkResponse response = client.bulk(bulkRequest);
				if (response.errors()) {
					System.out.println("Bulk operation failed with errors!");
					System.out.println(response);
				} else
					return;
			} catch (Exception e) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
		}

	}

	private void indexRecords(Path file, String index, FileSystem fileSystem) {

		try (InputStream is = new GZIPInputStream(fileSystem.open(file))) {
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
				String line;
				BulkRequest.Builder br = new BulkRequest.Builder();
				List<BulkOperation> operations = new ArrayList<>();
				while ((line = reader.readLine()) != null) {

					operations.add(createBulkOperation(line, index));
					if (operations.size() == 1000) {
						br.operations(operations);
						tryBulk(esClient, br.build(), 3);
						br = new BulkRequest.Builder();
						operations.clear();
					}

				}
				if (!operations.isEmpty()) {
					br.operations(operations);
					tryBulk(esClient, br.build(), 3);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void parallelBulkIndexScholix(final List<Path> files, String index, final int numberOfThreads,
		FileSystem fileSystem) {

		ForkJoinPool customThreadPool = new ForkJoinPool(numberOfThreads); // Set the desired level of parallelism
		customThreadPool.submit(() -> files.parallelStream().forEach(s -> indexRecords(s, index, fileSystem))).join();
	}

	public void refreshIndex(String indexName) throws IOException {
		esClient.indices().refresh(i -> i.index(indexName));
	}

	@Override
	public void close() throws IOException {
		esClient.close();
	}

}
