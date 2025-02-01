
package eu.dnetlib.dhp.person;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.logging.Filter;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.postgresql.shaded.com.ongres.scram.common.bouncycastle.pbkdf2.EncodableDigest;
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

public class SparkExtractPersonRelationsAndAddIndicators {

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
				addIndicators(spark, sourcePath, workingPath);
				removeIsolatedPerson(spark, sourcePath, workingPath);
			});
	}

	private static void addIndicators(SparkSession spark, String sourcePath, String workingPath) {
		//si leggono i result e si selezionano quelli con ordic.
		//per ogni result si prendono gli orcid value distinti e si emettono i downloads e citation count
		//si raggruppa per orcid e si sommano i vari contributi
		ModelSupport.entityTypes
				.keySet()
				.stream()
				.filter(ModelSupport::isResult)
				.forEach(
						e -> {
							// 1. search for results having orcid_pending and orcid in the set of pids for the authors
							spark
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
																							.asList("orcid", "orcid_pending")
																							.contains(p.getQualifier().getClassid().toLowerCase()))))
									.map((MapFunction<Result, ResultSubset>) ResultSubset::newInstance,Encoders.bean(ResultSubset.class))
									.write()
									.mode(SaveMode.Append)
									.option("compression", "gzip")
									.json(workingPath + "/resultWithPid");
						});

		Dataset<ResultSubset> resultSubset = spark.read().schema(Encoders.bean(ResultSubset.class).schema())
				.json(workingPath + "/resultWithPid")
				.as(Encoders.bean(ResultSubset.class));

		resultSubset
				.filter((FilterFunction<ResultSubset>) rs -> Optional.ofNullable(rs.getMeasures()).isPresent() )

				.flatMap((FlatMapFunction<ResultSubset, OrcidIndicators>) r -> {
							List<OrcidIndicators> oi = new ArrayList<>();
							r.getAuthor()
									.forEach(a -> {
												List<StructuredProperty> orcid = a.getPid().stream()
														.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid"))
														.collect(Collectors.toList());
												if (!orcid.isEmpty())
													oi.add(OrcidIndicators.newInstance(r.getId(), orcid.get(0).getValue(), r.getMeasures()));
												else
													oi.add(OrcidIndicators.newInstance(r.getId(), a.getPid().stream()
															.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid_pending"))
															.collect(Collectors.toList()).get(0).getValue(), r.getMeasures()));
											}
									);
							return oi.iterator();
						}
						, Encoders.bean(OrcidIndicators.class))
				.distinct()
				.groupByKey((MapFunction<OrcidIndicators, String>) OrcidIndicators::getOrcid, Encoders.STRING())
				.mapGroups((MapGroupsFunction<String, OrcidIndicators, OrcidIndicators>) (k, it) -> {
					OrcidIndicators acc = it.next();
					it.forEachRemaining(oi -> acc.addIndicators(oi.getDownloads(), oi.getCitations()));
					return acc;

				}, Encoders.bean(OrcidIndicators.class))
				.write()
				.mode(SaveMode.Append)
				.option("compression","gzip")
				.json(workingPath + "/orcidIndicators");

//		Dataset<Relation> relations = spark.read().schema(Encoders.bean(Relation.class).schema())
//				.json(sourcePath + "relation")
//				.as(Encoders.bean(Relation.class))
//				.filter((FilterFunction<Relation>) r -> !r.getDataInfo().getDeletedbyinference() && r.getRelClass().equalsIgnoreCase(ModelConstants.CITES));
//
//		Dataset<OrcidIndicators> citations = resultSubset.joinWith(relations, resultSubset.col("id").equalTo(relations.col("target")))
//				.flatMap((FlatMapFunction<Tuple2<ResultSubset, Relation>, OrcidIndicators>) t2 -> {
//					List<OrcidIndicators> oi = new ArrayList<>();
//					t2._1().getAuthor()
//							.forEach(a -> {
//										List<StructuredProperty> orcid = a.getPid().stream()
//												.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid"))
//												.collect(Collectors.toList());
//										if (!orcid.isEmpty())
//											oi.add(OrcidIndicators.newInstance(t2._1().getId(), orcid.get(0).getValue()));
//										else
//											oi.add(OrcidIndicators.newInstance(t2._1().getId(), a.getPid().stream()
//													.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid_pending"))
//													.collect(Collectors.toList()).get(0).getValue()));
//									}
//							);
//					return oi.iterator();
//				}, Encoders.bean(OrcidIndicators.class))
//				.groupByKey((MapFunction<OrcidIndicators, String>) oi -> oi.getOrcid(), Encoders.STRING())
//				.mapGroups((MapGroupsFunction<String, OrcidIndicators, OrcidIndicators>) (k, it) -> {
//							OrcidIndicators oi = it.next();
//							it.forEachRemaining(e -> oi.setCitations(oi.getCitations() + e.getCitations()));
//							return oi;
//						}, Encoders.bean(OrcidIndicators.class)
//				);
//
//		downloads.joinWith(citations, downloads.col("orcid").equalTo(citations.col("orcid")),"full")
//				.map((MapFunction<Tuple2<OrcidIndicators, OrcidIndicators>, OrcidIndicators>) t2 -> {
//					if(t2._1() == null)
//						return t2._2();
//					if(t2._2() == null)
//						return t2._1();
//					t2._1().setCitations(t2._2().getCitations());
//					return t2._1();
//
//				}, Encoders.bean(OrcidIndicators.class))
//				.write()
//				.mode(SaveMode.Overwrite)
//				.option("compression","gzip")
//				.json(workingPath + "/orcidIndicators");

		Dataset<Person> person = spark.read().schema(Encoders.bean(Person.class).schema())
				.json(sourcePath + "person")
				.as(Encoders.bean(Person.class));

		Dataset<OrcidIndicators> orcidIndicators = spark.read().schema(Encoders.bean(OrcidIndicators.class).schema())
				.json(workingPath + "/orcidIndicators")
				.as(Encoders.bean(OrcidIndicators.class));
//				.groupByKey((MapFunction<OrcidIndicators, String>) OrcidIndicators::getOrcid,Encoders.STRING() )
//				.mapGroups((MapGroupsFunction<String, OrcidIndicators, OrcidIndicators>) (k,it) -> {
//					OrcidIndicators acc = it.next();
//					it.forEachRemaining(oi -> acc.addIndicators(oi.getDownloads(),oi.getCitations()));
//					return acc;
//
//				},Encoders.bean(OrcidIndicators.class));

		person.joinWith(orcidIndicators, person.col("id").equalTo(orcidIndicators.col("orcid")),"left")
				.map((MapFunction<Tuple2<Person, OrcidIndicators>, Person>) t2 -> {
					Person p = t2._1();
					if(t2._2() != null) {
						p.setMeasures(Arrays.asList(getMeasure("downloads", String.valueOf(t2._2().getDownloads())),
								getMeasure("citations", String.valueOf(t2._2().getCitations()))));
					}
					return p;
				}, Encoders.bean(Person.class) )
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(workingPath + "/person");

		spark.read().schema(Encoders.bean(Person.class).schema())
				.json(workingPath + "/person")
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(sourcePath + "/person");

	}

	private static Measure getMeasure(String measureName, String measureValue){
		Measure measure = new Measure();
		measure.setId(measureName);
		KeyValue kv = new KeyValue();
		kv.setKey("score");
		kv.setValue(measureValue);
		measure.setUnit(Arrays.asList(kv));
		return measure;
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
													.asList("orcid", "orcid_pending")
													.contains(p.getQualifier().getClassid().toLowerCase()))));
					// 2. create authorship relations between the result identifier and the person entity with
					// orcid/orcid_pending.
					resultWithOrcids
						.flatMap(
							(FlatMapFunction<Result, Relation>) SparkExtractPersonRelationsAndAddIndicators::getAuthorshipRelations,
							Encoders.bean(Relation.class))
							.distinct()
						.write()
						.mode(SaveMode.Append)
						.option("compression", "gzip")
						.json(workingPath);

					// 3. create co_authorship relations between the pairs of authors with orcid/orcid_pending pids
					resultWithOrcids
						.map((MapFunction<Result, Coauthors>) SparkExtractPersonRelationsAndAddIndicators::getAuthorsPidList, Encoders.bean(Coauthors.class))
						.flatMap(
							(FlatMapFunction<Coauthors, Relation>) c -> new CoAuthorshipIterator(c.getCoauthors()),
							Encoders.bean(Relation.class))
						.distinct()
						.write()
						.mode(SaveMode.Append)
						.option("compression", "gzip")
						.json(workingPath);

				});
		spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(workingPath)
				.as(Encoders.bean(Relation.class))
				.distinct()
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
								p -> Arrays.asList("orcid", "orcid_pending").contains(p.getQualifier().getClassid())))
					.map(a -> {
						Optional<StructuredProperty> tmp = a
							.getPid()
							.stream()
							.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid"))
							.findFirst();
						if (tmp.isPresent())
							return tmp.get().getValue();
						tmp = a
							.getPid()
							.stream()
							.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid_pending"))
							.findFirst();
                        return tmp.map(StructuredProperty::getValue).orElse(null);

                    })
					.filter(Objects::nonNull)
					.collect(Collectors.toList()));
		return coauth;

	}

	private static Iterator<Relation> getAuthorshipRelations(Result r) {
		List<Relation> relationList = new ArrayList<>();
		List<StructuredProperty> orcids = new ArrayList<>();
		for (Author a : r.getAuthor()){
			orcids = a.getPid().stream().filter(p->p.getQualifier().getClassid().equalsIgnoreCase("orcid")).collect(Collectors.toList());
			if (orcids.isEmpty())
				orcids = a.getPid().stream().filter(p->p.getQualifier().getClassid().equalsIgnoreCase("orcid_pending")).collect(Collectors.toList());
			if(!orcids.isEmpty())
				relationList.add(getRelation(orcids.get(0).getValue(),r.getId()));

		}
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
