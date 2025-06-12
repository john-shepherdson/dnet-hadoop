
package eu.dnetlib.dhp.broker.oa.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.broker.oa.IndexEventSubsetJob;

public class ESIndexer {

	private final String indexHost;
	private final String indexName;
	private final String esBatchWriteRetryCount;
	private final String esBatchWriteRetryWait;
	private final String esBatchSizeEntries;
	private final String esNodesWanOnly;
	private final ObjectMapper objectMapper = new ObjectMapper();

	public ESIndexer(final String indexHost, final String indexName, final String esBatchWriteRetryCount, final String esBatchWriteRetryWait,
			final String esBatchSizeEntries,
			final String esNodesWanOnly) {
		this.indexHost = indexHost;
		this.indexName = indexName;
		this.esBatchWriteRetryCount = esBatchWriteRetryCount;
		this.esBatchWriteRetryWait = esBatchWriteRetryWait;
		this.esBatchSizeEntries = esBatchSizeEntries;
		this.esNodesWanOnly = esNodesWanOnly;
	}

	private static final Logger log = LoggerFactory.getLogger(IndexEventSubsetJob.class);

	public void performIndex(final JavaRDD<String> rdd, final String idField) {
		final Map<String, String> esCfg = new HashMap<>();

		esCfg.put("es.index.auto.create", "false");
		esCfg.put("es.nodes", this.indexHost);
		esCfg.put("es.mapping.id", idField); // THE PRIMARY KEY
		esCfg.put("es.batch.write.retry.count", this.esBatchWriteRetryCount);
		esCfg.put("es.batch.write.retry.wait", this.esBatchWriteRetryWait);
		esCfg.put("es.batch.size.entries", this.esBatchSizeEntries);
		esCfg.put("es.nodes.wan.only", this.esNodesWanOnly);

		log.info("*** Start indexing");
		JavaEsSpark.saveJsonToEs(rdd, this.indexName, esCfg);
		log.info("*** End indexing");

	}

	public <T> void performIndex(final Dataset<T> dataset, final String idField) {
		performIndex(dataset
				.map((MapFunction<T, String>) o -> this.objectMapper.writeValueAsString(o), Encoders.STRING())
				.toJavaRDD(), idField);
	}

}
