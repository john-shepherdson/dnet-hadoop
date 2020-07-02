
package eu.dnetlib.dhp.broker.oa;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;

public class IndexOnESJob {

	private static final Logger log = LoggerFactory.getLogger(IndexOnESJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					IndexOnESJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/index_es.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("workingPath") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String index = parser.get("index");
		log.info("index: {}", index);

		final String indexHost = parser.get("esHost");
		log.info("indexHost: {}", indexHost);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final JavaRDD<String> inputRdd = ClusterUtils
			.readPath(spark, eventsPath, Event.class)
			.limit(10000) // TODO REMOVE
			.map(IndexOnESJob::eventAsJsonString, Encoders.STRING())
			.javaRDD();

		final Map<String, String> esCfg = new HashMap<>();
		// esCfg.put("es.nodes", "10.19.65.51, 10.19.65.52, 10.19.65.53, 10.19.65.54");
		esCfg.put("es.nodes", indexHost);
		esCfg.put("es.mapping.id", "eventId"); // THE PRIMARY KEY
		esCfg.put("es.batch.write.retry.count", "8");
		esCfg.put("es.batch.write.retry.wait", "60s");
		esCfg.put("es.batch.size.entries", "200");
		esCfg.put("es.nodes.wan.only", "true");

		JavaEsSpark.saveJsonToEs(inputRdd, index, esCfg);
	}

	private static String eventAsJsonString(final Event f) throws JsonProcessingException {
		return new ObjectMapper().writeValueAsString(f);
	}

}
