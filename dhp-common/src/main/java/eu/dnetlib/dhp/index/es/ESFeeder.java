package eu.dnetlib.dhp.index.es;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * ESFeeder is a utility class for bulk indexing data into Elasticsearch using the Java API client.
 * It supports reading gzipped files, converting each line to a BulkOperation, and indexing in parallel.
 *
 * <p>Usage example:</p>
 * <pre>
 *   ESFeeder feeder = new ESFeeder("http://localhost:9200");
 *   feeder.parallelBulkIndex(files, "my-index", 4, fileSystem, converter);
 *   feeder.refreshIndex("my-index");
 *   feeder.close();
 * </pre>
 *
 * <ul>
 *   <li>Bulk operations are retried up to 3 times on failure.</li>
 *   <li>Supports parallel processing of multiple files using a custom thread pool.</li>
 * </ul>
 */
public class ESFeeder implements Closeable {

    private final ElasticsearchClient esClient;

    private final Logger logger  = LoggerFactory.getLogger(ESFeeder.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Constructs an ESFeeder with the given Elasticsearch URL.
     *
     * @param url the Elasticsearch endpoint (e.g., http://localhost:9200)
     */
    public ESFeeder(String url) {
        final RestClient client = RestClient
                .builder(
                        HttpHost.create(url))
                .setRequestConfigCallback(
                        conf -> conf
                                .setConnectTimeout(60000)
                                .setSocketTimeout(60000))
                .build();

        final ElasticsearchTransport transport = new RestClientTransport(
                client, new JacksonJsonpMapper());
        this.esClient = new ElasticsearchClient(transport);
    }

    private void tryBulk(ElasticsearchClient client, BulkRequest bulkRequest, int numberOfTry) {

        for (int i = 0; i < numberOfTry; i++) {
            try {
                BulkResponse response = client.bulk(bulkRequest);
                if (response.errors()) {
                    logger.error("Bulk operation failed with errors!");
                    logger.error(response.toString());
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

    /**
     * Indexes records from a gzipped file into the specified Elasticsearch index.
     * Each line is converted to a BulkOperation using the provided converter.
     * Operations are batched in groups of 1000.
     *
     * @param file the HDFS path to the gzipped file
     * @param index the Elasticsearch index name
     * @param fileSystem the Hadoop FileSystem instance
     * @param converter a function converting a line to a BulkOperation
     */
    private void indexRecords(Path file, String index, FileSystem fileSystem, Function<String, BulkOperation> converter) {

        try (InputStream is = new GZIPInputStream(fileSystem.open(file))) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                String line;
                BulkRequest.Builder br = new BulkRequest.Builder();
                List<BulkOperation> operations = new ArrayList<>();
                while ((line = reader.readLine()) != null) {
                    operations.add(converter.apply(line));
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

    /**
     * Performs parallel bulk indexing of multiple files.
     *
     * @param files list of HDFS file paths to index
     * @param index the Elasticsearch index name
     * @param numberOfThreads number of parallel threads to use
     * @param fileSystem the Hadoop FileSystem instance
     * @param converter a function converting a line to a BulkOperation
     */
    public void parallelBulkIndex(final List<Path> files, String index, final int numberOfThreads,
                                         FileSystem fileSystem, Function<String, BulkOperation> converter) {

        ForkJoinPool customThreadPool = new ForkJoinPool(numberOfThreads); // Set the desired level of parallelism
        customThreadPool.submit(() -> files.parallelStream().forEach(s -> indexRecords(s, index, fileSystem, converter))).join();
    }

    /**
     * Refreshes the specified Elasticsearch index to make all operations searchable.
     *
     * @param indexName the name of the index to refresh
     * @throws IOException if an I/O error occurs
     */
    public void refreshIndex(String indexName) throws IOException {
        esClient.indices().refresh(i -> i.index(indexName));
    }

    /**
     * Closes the Elasticsearch client and releases resources.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        esClient.close();
    }
}
