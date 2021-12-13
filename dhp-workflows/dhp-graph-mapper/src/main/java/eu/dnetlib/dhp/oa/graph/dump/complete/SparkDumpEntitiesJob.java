
package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.OafEntity;

/**
 * Spark Job that fires the dump for the entites
 */
public class SparkDumpEntitiesJob implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkDumpEntitiesJob.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpEntitiesJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/input_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final String communityMapPath = parser.get("communityMapPath");

		Class<? extends OafEntity> inputClazz = (Class<? extends OafEntity>) Class.forName(resultClassName);

		DumpGraphEntities dg = new DumpGraphEntities();
		dg.run(isSparkSessionManaged, inputPath, outputPath, inputClazz, communityMapPath);

	}

}
