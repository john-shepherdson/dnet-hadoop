
package eu.dnetlib.dhp.provision;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class SparkIndexCollectionOnES {

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkIndexCollectionOnES.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/provision/index_on_es.json")));
		parser.parseArgument(args);

		SparkConf conf = new SparkConf()
			.setAppName(SparkIndexCollectionOnES.class.getSimpleName())
			.setMaster(parser.get("master"));

		conf.set("spark.sql.shuffle.partitions", "4000");

		final String sourcePath = parser.get("sourcePath");
		final String index = parser.get("index");
		final String idPath = parser.get("idPath");
		final String cluster = parser.get("cluster");
		final String clusterJson = IOUtils
			.toString(DropAndCreateESIndex.class.getResourceAsStream("/eu/dnetlib/dhp/provision/cluster.json"));

		final Map<String, String> clusterMap = new ObjectMapper().readValue(clusterJson, Map.class);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<String> inputRdd = sc.textFile(sourcePath);

		Map<String, String> esCfg = new HashMap<>();
		esCfg.put("es.nodes", clusterMap.get(cluster));
		esCfg.put("es.mapping.id", idPath);
		esCfg.put("es.batch.write.retry.count", "8");
		esCfg.put("es.batch.write.retry.wait", "60s");
		esCfg.put("es.batch.size.entries", "200");
		esCfg.put("es.nodes.wan.only", "true");
		JavaEsSpark.saveJsonToEs(inputRdd, index, esCfg);
	}
}
