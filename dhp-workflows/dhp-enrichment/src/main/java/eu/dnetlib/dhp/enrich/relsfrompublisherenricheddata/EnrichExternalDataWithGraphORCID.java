
package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import static eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE;
import static eu.dnetlib.dhp.common.person.Constants.getKeyValues;
import static eu.dnetlib.dhp.common.person.Constants.getSerializationBean;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.cloudera.com.fasterxml.jackson.core.JsonProcessingException;
import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.common.person.*;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.PropagationConstant;
import eu.dnetlib.dhp.common.author.SparkEnrichWithOrcidAuthors;
import eu.dnetlib.dhp.orcidtoresultfromsemrel.OrcidAuthors;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.dhp.utils.ORCIDAuthorEnricherResult;
import eu.dnetlib.dhp.utils.OrcidAuthor;
import scala.Tuple2;

public class EnrichExternalDataWithGraphORCID extends SparkEnrichWithOrcidAuthors {
	private static final Logger log = LoggerFactory.getLogger(EnrichExternalDataWithGraphORCID.class);
	public static final DataInfo DATAINFO = OafMapperUtils
		.dataInfo(
			false,
			PROPAGATION_DATA_INFO_TYPE,
			true,
			false,
			OafMapperUtils
				.qualifier(
					PropagationConstant.PROPAGATION_AUTHORSHIP_CLASS_ID,
					PropagationConstant.PROPAGATION_AUTHORSHIP_CLASS_NAME,
					ModelConstants.DNET_PROVENANCE_ACTIONS,
					ModelConstants.DNET_PROVENANCE_ACTIONS),
			"0.85");

	public EnrichExternalDataWithGraphORCID(String propertyPath, String[] args, Logger log) {
		super(propertyPath, args, log);
	}

	public static void main(String[] args) throws Exception {

		// Create instance and run the Spark application
		EnrichExternalDataWithGraphORCID app = new EnrichExternalDataWithGraphORCID(
			"/eu/dnetlib/dhp/wf/subworkflows/enrich/orcid/enrich_graph_orcid_parameters.json", args, log);
		app.initialize().run();

	}

	// orcidPath is the path to the source for orcid. In this case the oaire graph
	// graphPath is the path to the publishers outcomes to be enriched
	@Override
	public void createTemporaryData(SparkSession spark, String graphPath, String orcidPath, String targetPath) {
		// Done only for publications since it is the input from the publishers which should be enriched
//creates tuple2 <doi, orcidauthorslist>
		Dataset<Row> orcidDnet = spark
			.read()
			.schema(Encoders.bean(Result.class).schema())
			.json(orcidPath + "/publication")
			.as(Encoders.bean(Result.class))
			.filter(
				(FilterFunction<Result>) r -> r.getAuthor() != null &&
					r
						.getAuthor()
						.stream()
						.anyMatch(
							a -> a.getPid() != null && a
								.getPid()
								.stream()
								.anyMatch(
									p -> p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID) ||
										p
											.getQualifier()
											.getClassid()
											.equalsIgnoreCase(ModelConstants.ORCID_PENDING))))
			.map((MapFunction<Result, Tuple2<String, OrcidAuthors>>) r -> {

				OrcidAuthors authors = getOrcidAuthorsList(r.getAuthor());

				return new Tuple2<>(r.getId(), authors);
			}, Encoders.tuple(Encoders.STRING(), Encoders.bean(OrcidAuthors.class)))
			.selectExpr("_1 as id", "_2.orcidAuthorList as orcid_authors");// in this case the id is the doi

		orcidDnet.write().mode(SaveMode.Overwrite).option("compression", "gzip").parquet(targetPath + "/graph_authors");

		Dataset<Row> df = spark
			.read()
			.schema(Constants.RESULT_MATCHED_SCHEMA)
			.json(graphPath) // the path to the publisher files
			.where("id is not null");

		Dataset<Row> authors = df
			.selectExpr("id", "explode(authors) as author")
			.selectExpr(
				"id", "author.fullname as fullname",
				"author.pids as pids",
				"author.affiliations as affiliations",
					"author.corresponding as corresponding",
					"author.contributor_roles as roles" )
			.map(
				(MapFunction<Row, Tuple2<String, Author>>) a -> new Tuple2<>(a.getAs("id"), getAuthor(a)),
				Encoders.tuple(Encoders.STRING(), Encoders.bean(Author.class)))
			.groupByKey((MapFunction<Tuple2<String, Author>, String>) t2 -> t2._1(), Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Tuple2<String, Author>, Tuple2<String, PublisherAuthors>>) (k, it) -> {
					PublisherAuthors pa = new PublisherAuthors();
					while (it.hasNext())
						pa.getPublisherAuthorList().add(it.next()._2());
					return new Tuple2<>(k, pa);
				}, Encoders.tuple(Encoders.STRING(), Encoders.bean(PublisherAuthors.class)))
			.selectExpr("_1 as id", "_2.publisherAuthorList as graph_authors");

		orcidDnet
			.join(authors, "id")
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.parquet(targetPath + "/publication_unmatched");

	}

	// graphPath is the path to the publisher file
	// targetPath is the path to the graph
	@Override
	public void generateGraph(SparkSession spark, String graphPath, String workingDir, String targetPath) {

		// creates new relations of authorship with the declared_affiliation property
		Dataset<Relation> newRelations = getNewRelations(spark, workingDir);
		newRelations.show(false);
		// redirects new relations versus representatives if any
		Dataset<Row> graph_relations = getMergesRelationships(spark, targetPath);
		Dataset<Relation> redirectedRels = redirectNewRelationsOnRepresentatives(newRelations, graph_relations);

		// create nco authorship relations (need to merge author with pids from enriched and graph
		Dataset<Row> matched = spark
			.read()
			.schema(Encoders.bean(ORCIDAuthorEnricherResult.class).schema())
			.parquet(workingDir + "/publication_matched")
			.selectExpr("id", "enriched_author");
		// gets new coAuthorship relations if any to build

		Dataset<Row> graph = spark.read().parquet(workingDir + "/graph_authors");


		Dataset<Relation> coAuthorshipRels = graph
			.joinWith(matched, graph.col("id").equalTo(matched.col("id")))
			.flatMap(
				(FlatMapFunction<Tuple2<Row, Row>, Relation>) EnrichExternalDataWithGraphORCID::coAuthorshipRels,
				Encoders.bean(Relation.class));

		// need to merge the relations with same source target and semantics
		mergeOldAndNewRelations(spark, targetPath, redirectedRels.union(coAuthorshipRels))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir + "/relation");

		// write the new relations in the relation folder
		spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(workingDir + "/relation")
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(targetPath + "/relation");

	}

	private static OrcidAuthors getOrcidAuthorsList(List<Author> authors) {
		OrcidAuthors oas = new OrcidAuthors();
		List<OrcidAuthor> tmp = authors
			.stream()
			.map(EnrichExternalDataWithGraphORCID::getOrcidAuthor)
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
		oas.setOrcidAuthorList(tmp);
		return oas;
	}

	private static OrcidAuthor getOrcidAuthor(Author a) {
		return Optional
			.ofNullable(getOrcid(a))
			.map(orcid -> new OrcidAuthor(orcid, a.getSurname(), a.getName(), a.getFullname(), null))
			.orElse(null);

	}

	private static String getOrcid(Row a) {
		List<Row> authorPids = a.getList(a.fieldIndex("pid"));
		return authorPids.stream().filter(p -> {
			Row qualifier = p.getAs("qualifier");
			return qualifier.getAs("classid").equals("orcid");
		}).findFirst().map(p -> (String) p.getAs("value")).orElse(authorPids.stream().filter(p -> {
			Row qualifier = p.getAs("qualifier");
			return qualifier.getAs("classid").equals("orcid_pending");
		}).findFirst().map(p -> (String) p.getAs("value")).orElse(null));

	}

	private static String getOrcid(Author a) {
		if (a.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID)))
			return a
				.getPid()
				.stream()
				.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID))
				.findFirst()
				.get()
				.getValue();
		if (a
			.getPid()
			.stream()
			.anyMatch(p -> p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID_PENDING)))
			return a
				.getPid()
				.stream()
				.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase(ModelConstants.ORCID_PENDING))
				.findFirst()
				.get()
				.getValue();
		return null;

	}

	private static Iterator<Relation> coAuthorshipRels(Tuple2<Row, Row> t2) {

		List<String> authorsList1 = t2
			._1()
			.<Row> getList(t2._1().fieldIndex("orcid_authors"))
			.stream()
			.map(a -> (String) a.getAs("orcid"))
			.collect(Collectors.toList());
		List<String> authorsList2 = t2
			._2()
			.<Row> getList(t2._2().fieldIndex("enriched_author"))
			.stream()
			.map(a -> getOrcid(a))
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
		authorsList1.addAll(authorsList2);

		List<Relation> relList = new ArrayList<>();
		new CoAuthorshipIterator(authorsList1).forEachRemaining(r -> relList.add(r));
		return relList.iterator();

	}

	private static Dataset<Relation> mergeOldAndNewRelations(SparkSession spark, String relationPath,
		Dataset<Relation> redirectedRelations) {
		return spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(relationPath + "/relation")
			.as(Encoders.bean(Relation.class))
			.union(redirectedRelations)
			.groupByKey(
				(MapFunction<Relation, String>) r -> r.getSource() + r.getRelClass() + r.getTarget(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Relation, Relation>) (k, it) -> {
				final Relation[] ret = {
					it.next()
				};
				it.forEachRemaining(r -> ret[0] = MergeUtils.mergeRelation(ret[0], r));
				return ret[0];
			}, Encoders.bean(Relation.class));
	}

	private static Dataset<Relation> redirectNewRelationsOnRepresentatives(Dataset<Relation> newRelations,
		Dataset<Row> graph_relations) {
		return newRelations
			.joinWith(graph_relations, newRelations.col("target").equalTo(graph_relations.col("target")), "left")
			.map((MapFunction<Tuple2<Relation, Row>, Relation>) t2 -> {
				if (t2._2() != null)
					t2._1().setTarget(t2._2().getAs("target"));
				return t2._1();
			}, Encoders.bean(Relation.class));
	}

	private static Dataset<Row> getMergesRelationships(SparkSession spark, String targetPath) {
		return spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(targetPath + "/relation")
			.filter("relClass = 'merges'")
			.select("source", "target");
	}

	private static Dataset<Relation> getNewRelations(SparkSession spark, String workingDir) {
		return spark
			.read()
			.schema(Encoders.bean(ORCIDAuthorEnricherResult.class).schema())
			.parquet(workingDir + "/publication_matched")
			.selectExpr("id", "enriched_author")
			.flatMap(
				(FlatMapFunction<Row, Relation>) EnrichExternalDataWithGraphORCID::getRelationsList,
				Encoders.bean(Relation.class));
	}

	private static Iterator<Relation> getRelationsList(Row r) {

		List<Relation> relationList = new ArrayList<>();

		List<Row> eauthors = r.getList(r.fieldIndex("enriched_author"));

		eauthors.forEach(author -> {
			List<Row> pids = author.getList(author.fieldIndex("pid"));

			List<Row> pidList = pids
				.stream()
				.filter(
					p -> {
						Row qualifier = p.getAs("qualifier");
						return ModelConstants.ORCID.equalsIgnoreCase(qualifier.getAs("classid"))
							|| ModelConstants.ORCID_PENDING.equalsIgnoreCase(qualifier.getAs("classid"));
					})
				.collect(Collectors.toList());
			pidList
				.forEach(
					p -> relationList
						.add(
							getRelations(
								r.getAs("id"),
								author.getList(author.fieldIndex("rawAffiliationString")),
								Constants.removePrefixUrl(p.getAs("value")))));

		});

		return relationList.iterator();
	}

	private static Relation getRelations(String id, List<String> rawAffiliationString, String orcid) {
		Relation rel = OafMapperUtils
			.getRelation(Constants.PERSON_PREFIX + Constants.SEPARATOR
				+ DHPUtils.md5(orcid), id,
				ModelConstants.RESULT_PERSON_RELTYPE, ModelConstants.RESULT_PERSON_SUBRELTYPE,
				ModelConstants.RESULT_PERSON_HASAUTHORED,
				null, DATAINFO, null);
		rawAffiliationString.forEach(raf -> {
            try {
				List<KeyValue> keyValueList = getKeyValues(raf);

				if(keyValueList.size() > 0) {
					if(!Optional.ofNullable(rel.getProperties()).isPresent()) {
						rel.setProperties(new ArrayList<>());
					}
					rel.getProperties().addAll(keyValueList);
				}

			} catch (IOException e) {
                throw new RuntimeException(e);
            }


		});

		return rel;
	}



	private static @NotNull Author getAuthor(Row a) throws JsonProcessingException {
		Author author = new Author();


		author.setFullname(a.getAs("fullname"));
		author.setName(a.getAs("firstname"));
		author.setSurname(a.getAs("lastname"));

		List<StructuredProperty> pids = new ArrayList<>();

		List<Row> publisherPids = new ArrayList<>();
		if (Optional.ofNullable(a.getAs("pids")).isPresent())
			publisherPids = a.getList(a.fieldIndex("pids"));

		publisherPids.forEach(pid -> pids.add(getPid(pid)));

		SerializationBean sb = getSerializationBean(a);

		author.setPid(pids);
		// in this case the rawaffiliation string is used as an accumulator to create relations
		// a little hack not to have to change the schema and /or the implementazion of the analysis method
		author.setRawAffiliationString(Arrays.asList(new ObjectMapper().writeValueAsString(sb)));
		return author;
	}

	private static @Nullable StructuredProperty getPid(Row pid) {
		return OafMapperUtils
			.structuredProperty(
				Constants.removePrefixUrl(pid.getAs("value")),
				OafMapperUtils
					.qualifier(
						pid.getAs("schema"),
						pid.getAs("schema"),
						ModelConstants.DNET_PID_TYPES,
						ModelConstants.DNET_PID_TYPES),
				null);
	}

}
