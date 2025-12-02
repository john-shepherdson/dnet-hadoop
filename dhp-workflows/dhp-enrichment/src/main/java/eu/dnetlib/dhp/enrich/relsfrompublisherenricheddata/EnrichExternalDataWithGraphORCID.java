
package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import static eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.common.person.Constants;

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
import eu.dnetlib.dhp.common.person.CoAuthorshipIterator;
import eu.dnetlib.dhp.orcidtoresultfromsemrel.OrcidAuthors;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.dhp.utils.ORCIDAuthorEnricherResult;
import eu.dnetlib.dhp.utils.OrcidAuthor;
import scala.Option;
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
			// selects only publications with doi since it is the only way to match with publisher data
			.filter(
				(FilterFunction<Result>) r -> r.getPid() != null &&
					r.getPid().stream().anyMatch(p -> p.getQualifier().getClassid().equalsIgnoreCase("doi")))
			// select only the results with at least the orcid for one author
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
			.flatMap((FlatMapFunction<Result, Tuple2<String, OrcidAuthors>>) r -> {
				List<Tuple2<String, OrcidAuthors>> t2 = new ArrayList<>();
				List<String> dois = r
					.getPid()
					.stream()
					.filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("doi"))
					.map(p -> p.getValue())
					.collect(Collectors.toList());
				OrcidAuthors authors = getOrcidAuthorsList(r.getAuthor());
				dois.forEach(doi -> t2.add(new Tuple2<>(doi, authors)));
				return t2.iterator();
			}, Encoders.tuple(Encoders.STRING(), Encoders.bean(OrcidAuthors.class)))
			.selectExpr("_1 as id", "_2.orcidAuthorList as orcid_authors");// in this case the id is the doi

		orcidDnet.write().mode(SaveMode.Overwrite).option("compression", "gzip").parquet(targetPath + "/graph_authors");
		orcidDnet.show(false);
		Dataset<Row> df = spark
			.read()
			.schema(Constants.PUBLISHER_INPUT_SCHEMA)
			.json(graphPath) // the path to the publisher files
			.where("doi is not null");

		Dataset<Row> authors = df
			.selectExpr("doi", "explode(authors) as author")
			.selectExpr(
				"doi", "author.name.full as fullname",
				"author.name.first as firstname",
				"author.name.last as lastname",
				"author.pids as pids",
				"author.matchings as affiliations",
					"author.corresponding as corresponding",
					"author.contributor_roles as roles" )
			.map(
				(MapFunction<Row, Tuple2<String, Author>>) a -> new Tuple2<>(a.getAs("doi"), getAuthor(a)),
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
//authors.show(false);
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

//non faccio la coAuthorship perche' ci potrebbero essere dei problemi nel numero di prodotti co-autorati (eventuali doppioni)
//		Dataset<Relation> coAuthorshipRels = graph
//			.joinWith(matched, graph.col("id").equalTo(matched.col("id")))
//			.flatMap(
//				(FlatMapFunction<Tuple2<Row, Row>, Relation>) EnrichExternalDataWithGraphORCID::coAuthorshipRels,
//				Encoders.bean(Relation.class));

		// need to merge the relations with same source target and semantics
		//mergeOldAndNewRelations(spark, targetPath, redirectedRels.union(coAuthorshipRels))
		mergeOldAndNewRelations(spark, targetPath, redirectedRels)
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

	private static String getValue(List<Row> authorPids, String classid){
		Optional<Row> tmp = authorPids.stream().filter(p -> {
			if (p == null)
				return false;
			Row qualifier = p.getAs("qualifier");
			return qualifier.getAs("classid").equals(classid);
		}).findFirst();
        return tmp.<String>map(row -> row.getAs("value")).orElse(null);
    }

	private static String getOrcid(Row a) {
		List<Row> authorPids = a.getList(a.fieldIndex("pid"));
		String value = getValue(authorPids, "orcid");
		return value != null ? value : getValue(authorPids, "orcid_pending");

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
				//done like this because variable used in lambda should be final or effectively final
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
					t2._1().setTarget(t2._2().getAs("source"));
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
			.selectExpr("id as doi", "enriched_author")
			.flatMap(
				(FlatMapFunction<Row, Relation>) EnrichExternalDataWithGraphORCID::getRelationsList,
				Encoders.bean(Relation.class));
	}

	private static Iterator<Relation> getRelationsList(Row r) {

		List<Relation> relationList = new ArrayList<>();

		List<Row> eauthors = r.getList(r.fieldIndex("enriched_author"));

		eauthors.forEach(author -> {

			List<Row> pids = author.getList(author.fieldIndex("pid"));

			List<Row> pidList = Optional.ofNullable(pids)
					.map(p -> p.stream().filter(pid -> {

						if(Optional.ofNullable(pid).isPresent() && Optional.ofNullable(pid.getAs("qualifier")).isPresent()){
							Row q = pid.getAs("qualifier");
							return ModelConstants.ORCID.equalsIgnoreCase(q.getAs("classid"))
									|| ModelConstants.ORCID_PENDING.equalsIgnoreCase(q.getAs("classid"));
						}
						return false;
					})

							.collect(Collectors.toList()))
				.orElse(new ArrayList<>())
				;
			pidList
				.forEach(
					p -> relationList
						.add(
							getRelations(
								Constants.removePrefixUrl(r.getAs("doi")),
								author.getList(author.fieldIndex("rawAffiliationString")),
								Constants.removePrefixUrl(p.getAs("value")))));

		});

		return relationList.iterator();
	}

	private static Relation getRelations(String doi, List<String> rawAffiliationString, String orcid) {
		Relation rel = OafMapperUtils
			.getRelation(Constants.PERSON_PREFIX + Constants.SEPARATOR
				+ DHPUtils.md5(orcid), "50|doi_________::" + DHPUtils.md5(doi),
				ModelConstants.RESULT_PERSON_RELTYPE, ModelConstants.RESULT_PERSON_SUBRELTYPE,
				ModelConstants.RESULT_PERSON_HASAUTHORED,
				null, DATAINFO, null);
		rawAffiliationString.forEach(raf -> {
            try {
                SerializationBean sb = new ObjectMapper().readValue(raf, SerializationBean.class);
				List<KeyValue> keyValueList = new ArrayList<>();
				KeyValue orcidPair = new KeyValue();
				orcidPair.setKey("orcid");
				orcidPair.setValue(orcid);
				keyValueList.add(orcidPair);
				if(Optional.ofNullable(sb.getCorresponding()).isPresent()) {
					KeyValue kv = new KeyValue();
					kv.setKey("corresponding");
					kv.setValue(String.valueOf(sb.getCorresponding()));
					keyValueList.add(kv);
				}
				if(!sb.getAffs().isEmpty()) {
					sb.getAffs().forEach(a -> {
						KeyValue kv = new KeyValue();
						kv.setKey("declared_affiliation");
						if (Optional.ofNullable(a.getRor()).isPresent())
							kv.setValue(a.getRor());
						else
							kv.setValue("OpenOrgs: " + a.getOpenOrgs());
						kv
								.setDataInfo(
										OafMapperUtils
												.dataInfo(
														false,
														"openaire:inference",
														true,
														false,
														null,
														String.valueOf(a.getConfidence())));
						keyValueList.add(kv);
					});
				}
				if(Optional.ofNullable(sb.getRoles()).isPresent()) {
					sb.getRoles().stream().forEach(r -> {
						KeyValue kv = new KeyValue();
						if(Optional.ofNullable(r.getRoleSchema()).isPresent() && Optional.ofNullable(r.getRoleValue()).isPresent()) {
							kv.setKey("role");
							kv.setValue(r.getRoleSchema() + " " + r.getRoleValue());
						}else {
							kv.setKey("role");
							kv.setValue(r.getRoleName());
						}
						keyValueList.add(kv);
					});
				}

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

	private static Author getAuthor(Row a) throws JsonProcessingException {
		Author author = new Author();

		author.setName(a.getAs("firstname"));
		author.setFullname(a.getAs("fullname"));
		author.setSurname(a.getAs("lastname"));
		List<StructuredProperty> pids = new ArrayList<>();
		List<String> serializationBean = new ArrayList<>();

		List<Row> publisherPids = new ArrayList<>();
		if (Optional.ofNullable(a.getAs("pids")).isPresent())
			publisherPids = a.getList(a.fieldIndex("pids"));

		publisherPids.forEach(pid -> pids.add(getPid(pid)));
		SerializationBean sb = new SerializationBean();
		List<Row> affiliations = a.getList(a.fieldIndex("affiliations"));
		sb.setAffs(Optional.ofNullable(affiliations)
				.map(v -> v.stream().map(
						aff -> {
							if(aff.getAs("status").equals("active")){
								SerializationOrg so = new SerializationOrg();
								if("ror".equalsIgnoreCase(aff.getAs("pid")))
									so.setRor(aff.getAs("value"));
								else
									so.setOpenOrgs(aff.getAs("value"));
								so.setConfidence(aff.getAs("confidence"));
								so.setName(aff.getAs("name"));
								so.setCountry(aff.getAs("country"));
								return so;
							}
							return null;
						}
				).filter(Objects::nonNull).collect(Collectors.toList()))
				.orElse(Collections.emptyList()));


		List<Row> roles = a.getList(a.fieldIndex("roles"));
		if(Optional.ofNullable(roles).isPresent())
			sb.setRoles(roles.stream().map(r -> {
				SerializationRoles sr = null;
				if(Optional.ofNullable(r.getAs("schema")).isPresent()){
					sr = new SerializationRoles();
					sr.setRoleSchema(r.getAs("schema"));
				}

				if(Optional.ofNullable(r.getAs("value")).isPresent()){
					if(sr == null)
						sr = new SerializationRoles();
					sr.setRoleValue(r.getAs("value"));
				}
				if(Optional.ofNullable(r.getAs("name")).isPresent()){
					if(sr == null)
						sr = new SerializationRoles();
					sr.setRoleName(r.getAs("name"));
				}
					return sr;
			}).filter(Objects::nonNull).collect(Collectors.toList()));

		Object val = a.getAs("corresponding");
		if (val != null) {
			if (val instanceof Boolean) {
				sb.setCorresponding((Boolean) val);
			} else if (val instanceof String) {
				sb.setCorresponding(Boolean.valueOf((String) val));
			}
		}

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
