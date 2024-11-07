
package eu.dnetlib.dhp.person;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.person.CoAuthorshipIterator;
import eu.dnetlib.dhp.common.person.Coauthors;
import eu.dnetlib.dhp.countrypropagation.SparkCountryPropagationJob;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import scala.Tuple2;

public class SparkExtractPersonRelations {

	private static final Logger log = LoggerFactory.getLogger(SparkCountryPropagationJob.class);
	private static final String PERSON_PREFIX = ModelSupport.getIdPrefix(Person.class) + "|orcid_______";

	public static final DataInfo DATAINFO = OafMapperUtils
		.dataInfo(
			false,
			"openaire",
			true,
			false,
			OafMapperUtils
				.qualifier(
					ModelConstants.SYSIMPORT_CROSSWALK_REPOSITORY,
					ModelConstants.SYSIMPORT_CROSSWALK_REPOSITORY,
					ModelConstants.DNET_PROVENANCE_ACTIONS,
					ModelConstants.DNET_PROVENANCE_ACTIONS),
			"0.85");

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkCountryPropagationJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/person/input_personpropagation_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String sourcePath = parser.get("sourcePath");
		log.info("sourcePath: {}", sourcePath);

		final String workingPath = parser.get("outputPath");
		log.info("workingPath: {}", workingPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {

				extractRelations(
					spark,
					sourcePath,
					workingPath);
				removeIsolatedPerson(spark, sourcePath, workingPath);
			});
	}

	private static void removeIsolatedPerson(SparkSession spark, String sourcePath, String workingPath) {
		Dataset<Person> personDataset = spark
			.read()
			.schema(Encoders.bean(Person.class).schema())
			.json(sourcePath + "person")
			.as(Encoders.bean(Person.class));

		Dataset<Relation> relationDataset = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(sourcePath + "relation")
			.as(Encoders.bean(Relation.class));

		personDataset
			.join(relationDataset, personDataset.col("id").equalTo(relationDataset.col("source")), "left_semi")
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(workingPath + "person");

		spark
			.read()
			.schema(Encoders.bean(Person.class).schema())
			.json(workingPath + "person")
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(sourcePath + "person");
	}

	private static void extractRelations(SparkSession spark, String sourcePath, String workingPath) {

		Dataset<Tuple2<String, Relation>> relationDataset = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(sourcePath + "relation")
			.as(Encoders.bean(Relation.class))
			.map(
				(MapFunction<Relation, Tuple2<String, Relation>>) r -> new Tuple2<>(
					r.getSource() + r.getRelClass() + r.getTarget(), r),
				Encoders.tuple(Encoders.STRING(), Encoders.bean(Relation.class)));

		ModelSupport.entityTypes
			.keySet()
			.stream()
			.filter(ModelSupport::isResult)
			.forEach(
				e -> {
					// 1. search for results having orcid_pending and orcid in the set of pids for the authors
					Dataset<Result> resultWithOrcids = spark
						.read()
						.schema(Encoders.bean(Result.class).schema())
						.json(sourcePath + e.name())
						.as(Encoders.bean(Result.class))
						.filter(
							(FilterFunction<Result>) r -> !r.getDataInfo().getDeletedbyinference() &&
								!r.getDataInfo().getInvisible() &&
								Optional
									.ofNullable(r.getAuthor())
									.isPresent())
						.filter(
							(FilterFunction<Result>) r -> r
								.getAuthor()
								.stream()
								.anyMatch(
									a -> Optional
										.ofNullable(
											a
												.getPid())
										.isPresent() &&
										a
											.getPid()
											.stream()
											.anyMatch(
												p -> Arrays
													.asList("eu/dnetlib/dhp/enrich/orcid", "orcid_pending")
													.contains(p.getQualifier().getClassid().toLowerCase()))));
					// 2. create authorship relations between the result identifier and the person entity with
					// orcid_pending.
					Dataset<Tuple2<String, Relation>> newRelations = resultWithOrcids
						.flatMap(
							(FlatMapFunction<Result, Relation>) r -> getAuthorshipRelations(r),
							Encoders.bean(Relation.class))
//							.groupByKey((MapFunction<Relation, String>) r-> r.getSource()+r.getTarget(), Encoders.STRING() )
//							.mapGroups((MapGroupsFunction<String, Relation, Relation>) (k,it) -> it.next(), Encoders.bean(Relation.class) )
						.map(
							(MapFunction<Relation, Tuple2<String, Relation>>) r -> new Tuple2<>(
								r.getSource() + r.getRelClass() + r.getTarget(), r),
							Encoders.tuple(Encoders.STRING(), Encoders.bean(Relation.class)));
					newRelations
						.joinWith(relationDataset, newRelations.col("_1").equalTo(relationDataset.col("_1")), "left")
						.map((MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, Relation>>, Relation>) t2 -> {
							if (t2._2() == null)
								return t2._1()._2();
							return null;
						}, Encoders.bean(Relation.class))
						.filter((FilterFunction<Relation>) r -> r != null)
						.write()
						.mode(SaveMode.Append)
						.option("compression", "gzip")
						.json(workingPath);

					// 2.1 store in a separate location the relation between the person and the pids for the result?

					// 3. create co_authorship relations between the pairs of authors with orcid/orcid_pending pids
					newRelations = resultWithOrcids
						.map((MapFunction<Result, Coauthors>) r -> getAuthorsPidList(r), Encoders.bean(Coauthors.class))
						.flatMap(
							(FlatMapFunction<Coauthors, Relation>) c -> new CoAuthorshipIterator(c.getCoauthors()),
							Encoders.bean(Relation.class))
						.groupByKey(
							(MapFunction<Relation, String>) r -> r.getSource() + r.getTarget(), Encoders.STRING())
						.mapGroups(
							(MapGroupsFunction<String, Relation, Relation>) (k, it) -> it.next(),
							Encoders.bean(Relation.class))
						.map(
							(MapFunction<Relation, Tuple2<String, Relation>>) r -> new Tuple2<>(
								r.getSource() + r.getRelClass() + r.getTarget(), r),
							Encoders.tuple(Encoders.STRING(), Encoders.bean(Relation.class)));
					newRelations
						.joinWith(relationDataset, newRelations.col("_1").equalTo(relationDataset.col("_1")), "left")
						.map((MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, Relation>>, Relation>) t2 -> {
							if (t2._2() == null)
								return t2._1()._2();
							return null;
						}, Encoders.bean(Relation.class))
						.filter((FilterFunction<Relation>) r -> r != null)
						.write()
						.mode(SaveMode.Append)
						.option("compression", "gzip")
						.json(workingPath);

				});
		spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(workingPath)
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(sourcePath + "relation");

	}

	private static Coauthors getAuthorsPidList(Result r) {
		Coauthors coauth = new Coauthors();
		coauth
			.setCoauthors(
				r
					.getAuthor()
					.stream()
					.filter(
						a -> a
							.getPid()
							.stream()
							.anyMatch(
								p -> Arrays.asList("eu/dnetlib/dhp/enrich/orcid", "orcid_pending").contains(p.getQualifier().getClassid())))
					.map(a -> {
						Optional<StructuredProperty> tmp = a
							.getPid()
							.stream()
							.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("eu/dnetlib/dhp/enrich/orcid"))
							.findFirst();
						if (tmp.isPresent())
							return tmp.get().getValue();
						tmp = a
							.getPid()
							.stream()
							.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid_pending"))
							.findFirst();
						if (tmp.isPresent())
							return tmp.get().getValue();

						return null;
					})
					.filter(Objects::nonNull)
					.collect(Collectors.toList()));
		return coauth;

	}

	private static Iterator<Relation> getAuthorshipRelations(Result r) {
		List<Relation> relationList = new ArrayList<>();
		for (Author a : r.getAuthor())

			relationList.addAll(a.getPid().stream().map(p -> {

				if (p.getQualifier().getClassid().equalsIgnoreCase("orcid_pending"))
					return getRelation(p.getValue(), r.getId());
				return null;
			})
				.filter(Objects::nonNull)
				.collect(Collectors.toList()));

		return relationList.iterator();
	}

	private static Relation getRelation(String orcid, String resultId) {

		String source = PERSON_PREFIX + "::" + IdentifierFactory.md5(orcid);

		Relation relation = OafMapperUtils
			.getRelation(
				source, resultId, ModelConstants.RESULT_PERSON_RELTYPE,
				ModelConstants.RESULT_PERSON_SUBRELTYPE,
				ModelConstants.RESULT_PERSON_HASAUTHORED,
				null, // collectedfrom = null
				DATAINFO,
				null);

		return relation;
	}

}
