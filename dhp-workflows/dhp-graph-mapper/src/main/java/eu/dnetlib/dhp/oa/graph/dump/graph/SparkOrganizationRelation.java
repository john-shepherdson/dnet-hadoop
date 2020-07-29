
package eu.dnetlib.dhp.oa.graph.dump.graph;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.Provenance;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Node;
import eu.dnetlib.dhp.schema.dump.oaf.graph.RelType;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class SparkOrganizationRelation implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkOrganizationRelation.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkOrganizationRelation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump_whole/input_organization_parameters.json"));

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

		final OrganizationMap organizationMap = new Gson()
			.fromJson(parser.get("organizationCommunityMap"), OrganizationMap.class);
		log.info("organization map : {}", new Gson().toJson(organizationMap));

		SparkConf conf = new SparkConf();
		AtomicReference<Set<String>> relationSet = null;

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				writeRelations(spark, extractRelation(spark, inputPath, organizationMap), outputPath, organizationMap);

			});

	}

	private static void writeRelations(SparkSession spark, Set<String> rels, String outputPath,
		OrganizationMap organizationMap) {

		List<eu.dnetlib.dhp.schema.dump.oaf.graph.Relation> relList = new ArrayList<>();

		rels.forEach(oId -> {
			organizationMap.get(oId).forEach(community -> {
				eu.dnetlib.dhp.schema.dump.oaf.graph.Relation direct = new eu.dnetlib.dhp.schema.dump.oaf.graph.Relation();
				eu.dnetlib.dhp.schema.dump.oaf.graph.Relation inverse = new eu.dnetlib.dhp.schema.dump.oaf.graph.Relation();
				String id = Utils.getContextId(community);
				direct.setSource(Node.newInstance(id, "context"));
				direct.setTarget(Node.newInstance(oId, ModelSupport.idPrefixEntity.get(oId.substring(0, 2))));
				direct.setReltype(RelType.newInstance(ModelConstants.IS_RELATED_TO, ModelConstants.RELATIONSHIP));
				direct.setProvenance(Provenance.newInstance("Harvested", "0.9"));
				relList.add(direct);
				inverse.setTarget(Node.newInstance(id, "context"));
				inverse.setSource(Node.newInstance(oId, ModelSupport.idPrefixEntity.get(oId.substring(0, 2))));
				inverse.setReltype(RelType.newInstance(ModelConstants.IS_RELATED_TO, ModelConstants.RELATIONSHIP));
				inverse.setProvenance(Provenance.newInstance("Harvested", "0.9"));
				relList.add(inverse);

			});

		});

		spark
			.createDataset(relList, Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Relation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static Set<String> extractRelation(SparkSession spark, String inputPath, OrganizationMap organizationMap) {
		Dataset<Relation> tmp = Utils.readPath(spark, inputPath, Relation.class);
		Set<String> organizationSet = organizationMap.keySet();
		Set<String> toCreateRels = new HashSet<>();

		tmp.foreach((ForeachFunction<Relation>) relation -> {
			Optional<DataInfo> odInfo = Optional.ofNullable(relation.getDataInfo());
			if (odInfo.isPresent()) {
				if (!odInfo.get().getDeletedbyinference()) {
					if (relation.getRelClass().equals(ModelConstants.MERGES)) {
						String oId = relation.getTarget();
						if (organizationSet.contains(oId)) {
							organizationSet.remove(oId);
							toCreateRels.add(relation.getSource());
						}
					}
				}
			}
		});

		toCreateRels.addAll(organizationSet);
		return toCreateRels;

	}

}
