
package eu.dnetlib.dhp.oa.graph.dump.graph;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Node;
import eu.dnetlib.dhp.schema.dump.oaf.graph.RelType;
import eu.dnetlib.dhp.schema.oaf.Relation;

/**
 * Dumps eu.dnetlib.dhp.schema.oaf.Relation in eu.dnetlib.dhp.schema.dump.oaf.graph.Relation
 */
public class SparkDumpRelationJob implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkDumpRelationJob.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpRelationJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_whole/input_relationdump_parameters.json"));

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

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				dumpRelation(spark, inputPath, outputPath);

			});

	}

	private static void dumpRelation(SparkSession spark, String inputPath, String outputPath) {
		Utils
			.readPath(spark, inputPath, Relation.class)
			.map(relation -> {
				eu.dnetlib.dhp.schema.dump.oaf.graph.Relation rel = new eu.dnetlib.dhp.schema.dump.oaf.graph.Relation();
				rel
					.setSource(
						Node
							.newInstance(
								relation.getSource(),
								ModelSupport.idPrefixEntity.get(relation.getSource().substring(0, 2))));

				rel
					.setTarget(
						Node
							.newInstance(
								relation.getTarget(),
								ModelSupport.idPrefixEntity.get(relation.getTarget().substring(0, 2))));

				rel
					.setReltype(
						RelType
							.newInstance(
								relation.getRelClass(),
								relation.getSubRelType()));

				Optional
					.ofNullable(relation.getDataInfo())
					.ifPresent(
						datainfo -> rel
							.setProvenance(
								Provenance
									.newInstance(datainfo.getProvenanceaction().getClassname(), datainfo.getTrust())));

				return rel;

			}, Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Relation.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);

	}

}
