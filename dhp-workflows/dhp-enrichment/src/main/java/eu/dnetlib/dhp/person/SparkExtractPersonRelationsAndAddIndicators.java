
package eu.dnetlib.dhp.person;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.logging.Filter;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.common.person.Constants;
import eu.dnetlib.dhp.schema.oaf.rel.Authorship;
import eu.dnetlib.dhp.schema.oaf.rel.CoAuthorship;
import eu.dnetlib.dhp.schema.oaf.rel.beans.AuthorshipRoles;
import eu.dnetlib.dhp.schema.oaf.rel.beans.DeclaredAffiliation;
import eu.dnetlib.dhp.schema.oaf.rel.beans.Role;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
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
import static org.apache.spark.sql.functions.*;

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

		String sourcePath = parser.get("sourcePath") + "/";
		log.info("sourcePath: {}", sourcePath);

		final String workingPath = parser.get("outputPath");
		log.info("workingPath: {}", workingPath);

		final String matchingDataset = parser.get("matchingDataset");
		log.info("matchingDataset: {}", matchingDataset);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {

				extractRelations(
					spark,
					sourcePath,
					workingPath, matchingDataset);
				addIndicators(spark, sourcePath, workingPath);
				removeIsolatedPerson(spark, sourcePath, workingPath);
			});
	}

	private static void addIndicators(SparkSession spark, String sourcePath, String workingPath) {
		// si leggono i result e si selezionano quelli con ordic.
		// per ogni result si prendono gli orcid value distinti e si emettono i downloads e citation count
		// si raggruppa per orcid e si sommano i vari contributi

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
						.map(
							(MapFunction<Result, ResultSubset>) ResultSubset::newInstance,
							Encoders.bean(ResultSubset.class))
						.write()
						.mode(SaveMode.Append)
						.option("compression", "gzip")
						.json(workingPath + "/resultWithPid");
				});

		Dataset<ResultSubset> resultSubset = spark
			.read()
			.schema(Encoders.bean(ResultSubset.class).schema())
			.json(workingPath + "/resultWithPid")
			.as(Encoders.bean(ResultSubset.class));

		resultSubset
			.filter((FilterFunction<ResultSubset>) rs -> Optional.ofNullable(rs.getMeasures()).isPresent())

			.flatMap((FlatMapFunction<ResultSubset, OrcidIndicators>) r -> {
				List<OrcidIndicators> oi = new ArrayList<>();
				r
					.getAuthor()
					.forEach(a -> {
						List<StructuredProperty> orcid = a
							.getPid()
							.stream()
							.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid"))
							.collect(Collectors.toList());
						if (!orcid.isEmpty())
							oi.add(OrcidIndicators.newInstance(r.getId(), orcid.get(0).getValue(), r.getMeasures()));
						else {
							orcid = a
								.getPid()
								.stream()
								.filter(
									p -> p
										.getQualifier()
										.getClassid()
										.equalsIgnoreCase("orcid_pending"))
								.collect(Collectors.toList());
							if (!orcid.isEmpty())
								oi
									.add(
										OrcidIndicators
											.newInstance(
												r.getId(),
												orcid
													.get(0)
													.getValue(),
												r.getMeasures()));

						}

					});
				return oi.iterator();
			}, Encoders.bean(OrcidIndicators.class))
			.distinct()
			.groupByKey((MapFunction<OrcidIndicators, String>) OrcidIndicators::getOrcid, Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, OrcidIndicators, OrcidIndicators>) (k, it) -> {
				OrcidIndicators acc = it.next();
				it.forEachRemaining(oi -> acc.addIndicators(oi.getDownloads(), oi.getCitations()));
				return acc;

			}, Encoders.bean(OrcidIndicators.class))
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(workingPath + "/orcidIndicators");


		Dataset<Person> person = spark
			.read()
			.schema(Encoders.bean(Person.class).schema())
			.json(sourcePath + "person")
			.as(Encoders.bean(Person.class));

		Dataset<OrcidIndicators> orcidIndicators = spark
			.read()
			.schema(Encoders.bean(OrcidIndicators.class).schema())
			.json(workingPath + "/orcidIndicators")
			.as(Encoders.bean(OrcidIndicators.class));

		person
			.joinWith(orcidIndicators, person.col("id").equalTo(orcidIndicators.col("orcid")), "left")
			.map((MapFunction<Tuple2<Person, OrcidIndicators>, Person>) t2 -> {
				Person p = t2._1();
				if (t2._2() != null) {
					p
						.setMeasures(
							Arrays
								.asList(
									getMeasure("downloads", String.valueOf(t2._2().getDownloads())),
									getMeasure("citations", String.valueOf(t2._2().getCitations()))));
				}
				return p;
			}, Encoders.bean(Person.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath + "/person");

		spark
			.read()
			.schema(Encoders.bean(Person.class).schema())
			.json(workingPath + "/person")
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(sourcePath + "person");

	}

	private static Measure getMeasure(String measureName, String measureValue) {
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

	private static void extractRelations(SparkSession spark, String sourcePath, String workingPath, String matchingDataset) {

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
							(FlatMapFunction<Result, Authorship>) SparkExtractPersonRelationsAndAddIndicators::getAuthorshipRelations,
							Encoders.bean(Authorship.class))
						.distinct()
						.write()
						.mode(SaveMode.Append)
						.option("compression", "gzip")
						.json(workingPath + "/authorship");

					// 3. create co_authorship relations between the pairs of authors with orcid/orcid_pending pids
					resultWithOrcids
						.map(
							(MapFunction<Result, Coauthors>) SparkExtractPersonRelationsAndAddIndicators::getAuthorsPidList,
							Encoders.bean(Coauthors.class))
						.flatMap(
							(FlatMapFunction<Coauthors, CoAuthorship>) c -> new CoAuthorshipIterator(c.getCoauthors()),
							Encoders.bean(CoAuthorship.class))
							.groupByKey((MapFunction<CoAuthorship, String>) r -> r.getAuthor1() + "::" + r.getAuthor2(), Encoders.STRING() )
							.mapGroups((MapGroupsFunction<String, CoAuthorship, CoAuthorship>) (k,it) -> {
								CoAuthorship ca = it.next();
								it.forEachRemaining(r -> ca.setCoauthoredProducts(ca.getCoauthoredProducts() + r.getCoauthoredProducts() ));
								return ca;
							} , Encoders.bean(CoAuthorship.class))
						.write()
						.mode(SaveMode.Append)
						.option("compression", "gzip")
						.json(workingPath + "/coauthorship");

				});

		spark
			.read()
			.schema(Encoders.bean(Authorship.class).schema())
			.json(workingPath)
			.as(Encoders.bean(Authorship.class))
				.groupByKey((MapFunction<Authorship, String>) a -> a.getPerson() + "::"  + a.getProduct(), Encoders.STRING())
				.mapGroups((MapGroupsFunction<String, Authorship, Authorship>) (k,it) -> {
					Authorship authorship = it.next();
					while(it.hasNext())
						authorship = mergeAuthorship(authorship, it.next());

					return  authorship;
		}, Encoders.bean(Authorship.class))
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(sourcePath + "relation");

	}

	private static Authorship mergeAuthorship(Authorship acc, Authorship toMerge){
		if(acc == null)
			return toMerge;
		if(toMerge == null)
			return acc;
		if(acc.getRoles().isEmpty())
			acc.setRoles(toMerge.getRoles());
		else{
			if(Optional.ofNullable(toMerge.getRoles()).isPresent())
					toMerge.getRoles().forEach(role -> addRole(acc.getRoles(),role));
		}
		if(acc.getCorresponding() == null)
			acc.setCorresponding(toMerge.getCorresponding());
		if(acc.getDeclaredAffiliations() == null)
			acc.setDeclaredAffiliations(toMerge.getDeclaredAffiliations());
		else{
			if(Optional.ofNullable(toMerge.getDeclaredAffiliations()).isPresent())
				toMerge.getDeclaredAffiliations().forEach(affiliation -> addAffiliation(acc.getDeclaredAffiliations(), affiliation));
		}
		return acc;
	}

	private static void addAffiliation(List<DeclaredAffiliation> declaredAffiliations, DeclaredAffiliation affiliation) {
	}

	private static void addRole(List<Role> roles, Role role){
		
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


	private static Iterator<Authorship> getAuthorshipRelations(Result r) {
		List<Authorship> relationList = new ArrayList<>();
		List<StructuredProperty> orcids = new ArrayList<>();

		for (Author a : r.getAuthor()) {
			orcids = a.getPid().stream().filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid"))
					.collect(Collectors.toList());
			if (orcids.isEmpty())
				orcids = a
					.getPid()
					.stream()
					.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("orcid_pending"))
					.collect(Collectors.toList());
			if (!orcids.isEmpty())
				relationList.add(getAuthorshipRelation(orcids.get(0).getValue(), r.getId(), a.getRank(), a.getRawAffiliationString()));

		}
		return relationList.iterator();
	}

	private static Authorship getAuthorshipRelation(String orcid, String resultId, Integer rank, List<String> rawAffiliations) {
		String source = Constants.getPersonId(orcid);
		Authorship authorship = new Authorship();
		authorship.setPerson(source);
		authorship.setProduct(resultId);
		authorship.setRank(rank);
		authorship.setDataInfo(DATAINFO);
		//Maybe we should not add the string without a matching organization
		//not in the relation anyway
		authorship.setDeclaredAffiliations(rawAffiliations.stream().map(rawAffiliation -> {
			DeclaredAffiliation da = new DeclaredAffiliation();
			da.setRawAffiliation(rawAffiliation);
			return da;
		}).collect(Collectors.toList()));
		return authorship;
	}



}
