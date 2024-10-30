
package eu.dnetlib.dhp.oa.merge;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

/**
 * Copy specified entities from a graph snapshot to another
 */
public class CopyEntitiesSparkJob {
	private static final Logger log = LoggerFactory.getLogger(CopyEntitiesSparkJob.class);

	private ArgumentApplicationParser parser;

	public CopyEntitiesSparkJob(ArgumentApplicationParser parser) {
		this.parser = parser;
	}

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				CopyEntitiesSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/merge/copy_graph_entities_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		new CopyEntitiesSparkJob(parser).run(isSparkSessionManaged);
	}

	public void run(Boolean isSparkSessionManaged)
		throws ISLookUpException {

		String graphInputPath = parser.get("graphInputPath");
		log.info("graphInputPath: {}", graphInputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String entities = parser.get("entities");
		log.info("entities: {}", entities);

		String format = parser.get("format");
		log.info("format: {}", format);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Arrays
					.stream(entities.split(","))
					.map(x -> x.trim().toLowerCase())
					.filter(ModelSupport.oafTypes::containsKey)
					.forEachOrdered(
						entity -> {
							switch (format.toLowerCase()) {
								case "text":
									spark
										.read()
										.text(graphInputPath + "/" + entity)
										.write()
										.option("compression", "gzip")
										.mode("overwrite")
										.text(outputPath + "/" + entity);
									break;
								case "json":
									spark
										.read()
										.json(graphInputPath + "/" + entity)
										.write()
										.option("compression", "gzip")
										.mode("overwrite")
										.json(outputPath + "/" + entity);
									break;
								case "parquet":
									spark
										.read()
										.parquet(graphInputPath + "/" + entity)
										.write()
										.option("compression", "gzip")
										.mode("overwrite")
										.parquet(outputPath + "/" + entity);
									break;
							}
						});
			});
	}
}
