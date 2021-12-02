
package eu.dnetlib.dhp.oa.graph.dump.complete;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
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
import eu.dnetlib.dhp.schema.oaf.DataInfo;
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
							"/eu/dnetlib/dhp/oa/graph/dump/input_relationdump_parameters.json"));

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

		Optional<String> rs  = Optional.ofNullable(parser.get("removeSet"));
		final Set<String> removeSet = new HashSet<>();
		if(rs.isPresent()){
			Collections.addAll(removeSet, rs.get().split(";"));
		}


		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				dumpRelation(spark, inputPath, outputPath, removeSet);

			});

	}

	private static void dumpRelation(SparkSession spark, String inputPath, String outputPath, Set<String> removeSet) {
		Dataset<Relation> relations = Utils.readPath(spark, inputPath, Relation.class);
		relations
				.filter((FilterFunction<Relation>)r -> !removeSet.contains(r.getRelClass()))
			.map((MapFunction<Relation, eu.dnetlib.dhp.schema.dump.oaf.graph.Relation>) relation -> {
				eu.dnetlib.dhp.schema.dump.oaf.graph.Relation relNew = new eu.dnetlib.dhp.schema.dump.oaf.graph.Relation();
				relNew
					.setSource(
						Node
							.newInstance(
								relation.getSource(),
								ModelSupport.idPrefixEntity.get(relation.getSource().substring(0, 2))));

				relNew
					.setTarget(
						Node
							.newInstance(
								relation.getTarget(),
								ModelSupport.idPrefixEntity.get(relation.getTarget().substring(0, 2))));

				relNew
					.setReltype(
						RelType
							.newInstance(
								relation.getRelClass(),
								relation.getSubRelType()));

				Optional<DataInfo> odInfo = Optional.ofNullable(relation.getDataInfo());
				if (odInfo.isPresent()) {
					DataInfo dInfo = odInfo.get();
					if (Optional.ofNullable(dInfo.getProvenanceaction()).isPresent() &&
						Optional.ofNullable(dInfo.getProvenanceaction().getClassname()).isPresent()) {
						relNew
							.setProvenance(
								Provenance
									.newInstance(
										dInfo.getProvenanceaction().getClassname(),
										dInfo.getTrust()));
					}
				}
				if (Boolean.TRUE.equals(relation.getValidated())) {
					relNew.setValidated(relation.getValidated());
					relNew.setValidationDate(relation.getValidationDate());
				}

				return relNew;

			}, Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Relation.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);

	}

}
