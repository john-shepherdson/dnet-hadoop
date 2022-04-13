
package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.KeyValueSet;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class PrepareResultInstRepoAssociation {

	private static final Logger log = LoggerFactory.getLogger(PrepareResultInstRepoAssociation.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareResultInstRepoAssociation.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/input_prepareresultorg_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		List<String> blacklist = Optional
			.ofNullable(parser.get("blacklist"))
			.map(v -> Arrays.asList(v.split(";")))
			.orElse(new ArrayList<>());

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				readNeededResources(spark, inputPath, workingPath, blacklist);

				prepareDatasourceOrganization(spark, workingPath);

				prepareAlreadyLinkedAssociation(spark, workingPath);
			});
	}

	private static void readNeededResources(SparkSession spark, String inputPath, String workingPath,
		List<String> blacklist) {
		readPath(spark, inputPath + "/datasource", Datasource.class)
			.filter(
				(FilterFunction<Datasource>) ds -> !blacklist.contains(ds.getId()) &&
					!ds.getDataInfo().getDeletedbyinference() &&
					ds.getDatasourcetype().getClassid().equals(INSTITUTIONAL_REPO_TYPE))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath + "/datasource");

		readPath(spark, inputPath + "/relation", Relation.class)
			.filter(
				(FilterFunction<Relation>) r -> !r.getDataInfo().getDeletedbyinference() &&
					(r.getRelClass().toLowerCase().equals(ModelConstants.IS_PROVIDED_BY.toLowerCase()) ||
						r.getRelClass().toLowerCase().equals(ModelConstants.HAS_AUTHOR_INSTITUTION.toLowerCase())))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath + "/relation");
	}

	private static void prepareDatasourceOrganization(
		SparkSession spark, String workingPath) {

		Dataset<Datasource> datasource = readPath(spark, workingPath + "/datasource", Datasource.class);

		Dataset<Relation> relation = readPath(spark, workingPath + "/relation", Relation.class)
			.filter(
				(FilterFunction<Relation>) r -> r
					.getRelClass()
					.toLowerCase()
					.equals(ModelConstants.IS_PROVIDED_BY.toLowerCase()));

		datasource
			.joinWith(relation, datasource.col("id").equalTo(relation.col("source")))
			.map(
				(MapFunction<Tuple2<Datasource, Relation>, DatasourceOrganization>) t2 -> DatasourceOrganization
					.newInstance(t2._2().getSource(), t2._2().getTarget()),
				Encoders.bean(DatasourceOrganization.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath + "/ datasourceOrganization");
		;

	}

	private static void prepareAlreadyLinkedAssociation(
		SparkSession spark, String workingPath) {

		readPath(spark, workingPath + "/relation", Relation.class)
			.filter(
				(FilterFunction<Relation>) r -> r
					.getRelClass()
					.toLowerCase()
					.equals(ModelConstants.HAS_AUTHOR_INSTITUTION.toLowerCase()))
			.groupByKey((MapFunction<Relation, String>) r -> r.getSource(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Relation, KeyValueSet>) (k, it) -> {
				Set<String> values = new HashSet<>();
				KeyValueSet kvs = new KeyValueSet();
				kvs.setKey(k);
				values.add(it.next().getTarget());
				it.forEachRemaining(r -> values.add(r.getTarget()));
				kvs.setValueSet(new ArrayList<>(values));
				return kvs;
			}, Encoders.bean(KeyValueSet.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath + "/alreadyLinked");

	}

}
