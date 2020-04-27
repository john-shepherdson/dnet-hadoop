
package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

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
		final String type = parser.get("type");

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<String> inputRdd;

		if ("summary".equalsIgnoreCase(type))
			inputRdd = spark
				.read()
				.load(sourcePath)
				.as(Encoders.bean(ScholixSummary.class))
				.map(
					(MapFunction<ScholixSummary, String>) f -> {
						final ObjectMapper mapper = new ObjectMapper();
						return mapper.writeValueAsString(f);
					},
					Encoders.STRING())
				.javaRDD();
		else
			inputRdd = sc.textFile(sourcePath);

		Map<String, String> esCfg = new HashMap<>();
		esCfg.put("es.nodes", "10.19.65.51, 10.19.65.52, 10.19.65.53, 10.19.65.54");
		esCfg.put("es.mapping.id", idPath);
		esCfg.put("es.batch.write.retry.count", "8");
		esCfg.put("es.batch.write.retry.wait", "60s");
		esCfg.put("es.batch.size.entries", "200");
		esCfg.put("es.nodes.wan.only", "true");
		JavaEsSpark.saveJsonToEs(inputRdd, index, esCfg);
	}
}
