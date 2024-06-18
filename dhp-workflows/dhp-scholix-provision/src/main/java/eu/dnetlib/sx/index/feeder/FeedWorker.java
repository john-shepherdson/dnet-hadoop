
package eu.dnetlib.sx.index.feeder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.sx.index.RequestManager;

public class FeedWorker extends Thread {

	private final FileSystem fileSystem;

	public static String JOB_COMPLETE = "JOB_COMPLETE";

	private final BlockingQueue<String> queue;

	private final Logger log = LoggerFactory.getLogger(getClass().getName());

	private boolean hasComplete = false;
	private final String index;

	private final RequestManager requestCreator;

	private final RestHighLevelClient client;

	public FeedWorker(
		FileSystem fileSystem, BlockingQueue<String> queue,
		RequestManager requestCreator, final String host, final String index) {
		this.fileSystem = fileSystem;
		this.queue = queue;
		this.index = index;
		this.requestCreator = requestCreator;
		this.client = createRESTClient(host);
	}

	private RestHighLevelClient createRESTClient(String host) {
		System.out.println("Creating client with host = " + host);

		return new RestHighLevelClient(
			RestClient.builder(new HttpHost(host, 9200, "http")));
	}

	@Override
	public void run() {
		CompressionCodecFactory factory = new CompressionCodecFactory(fileSystem.getConf());
		while (!hasComplete) {
			try {
				final String nextItem = queue.take();
				if (JOB_COMPLETE.equalsIgnoreCase(nextItem)) {
					hasComplete = true;
				} else {
					System.out.println("Parsing " + nextItem + "\n");
					final Path currentPath = new Path(nextItem);

					final CompressionCodec codec = factory.getCodec(currentPath);
					InputStream gzipInputStream = codec.createInputStream(fileSystem.open(currentPath));
					Reader decoder = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
					BufferedReader reader = new BufferedReader(decoder);
					doIndexChunk(reader);
				}

			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		}

	}

	private BulkRequest createRequest() {
		final BulkRequest request = new BulkRequest();
		request.timeout("2m");
		return request;

	}

	private void doIndexChunk(final BufferedReader reader) throws Exception {
		String next;
		BulkRequest request = createRequest();
		int i = 0;
		while ((next = reader.readLine()) != null) {
			request.add(this.requestCreator.createRequest(next, index));
			if (i++ % 10000 == 0) {
				client.bulk(request, RequestOptions.DEFAULT);
				request = createRequest();
				System.out.printf("Bulk->  %d items \n", i);
				log.debug("Bulk->  {} items ", i);
			}
		}
		client.bulk(request, RequestOptions.DEFAULT);
		System.out.printf(" Final Bulk->  %d items \n", i);
		log.debug("Final Bulk->  {} items ", i);
	}
}
