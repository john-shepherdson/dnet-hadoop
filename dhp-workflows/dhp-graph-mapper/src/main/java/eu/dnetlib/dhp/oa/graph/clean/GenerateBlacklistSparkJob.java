
package eu.dnetlib.dhp.oa.graph.clean;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class GenerateBlacklistSparkJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateBlacklistSparkJob.class);

	private ArgumentApplicationParser parser;

	public GenerateBlacklistSparkJob(ArgumentApplicationParser parser) {
		this.parser = parser;
	}

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				GenerateBlacklistSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/generate_blacklist_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		new GenerateBlacklistSparkJob(parser).run(isSparkSessionManaged);
	}

	public void run(Boolean isSparkSessionManaged)
		throws ISLookUpException, ClassNotFoundException {

		String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String zenodoWithdrawn = parser.get("zenodoWithdrawn");
		log.info("zenodoWithdrawn: {}", zenodoWithdrawn);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());

				Dataset<Row> zenodo_withdrawn_dois = spark.read().load(zenodoWithdrawn);

				for (Map.Entry<String, Class> e : ModelSupport.oafTypes.entrySet()) {
					Class<?> clazz = e.getValue();
					if (Result.class.isAssignableFrom(clazz)) {
						spark
							.read()
							.schema(Encoders.bean(Result.class).schema())
							.json(inputPath + "/" + e.getKey())
							.where("array_contains(instance.hostedby.value, 'ZENODO')")
							.selectExpr("id", "explode(instance) as instance")
							.selectExpr("id", "explode(instance.pid) as pid")
							.join(
								zenodo_withdrawn_dois,
								zenodo_withdrawn_dois.col("doi").equalTo(new Column("pid.value")), "left_semi")
							.select("id")
							.distinct()
							.write()
							.mode("append")
							.option("compression", "gzip")
							.parquet(outputPath);
					}
				}
			});
	}
}
