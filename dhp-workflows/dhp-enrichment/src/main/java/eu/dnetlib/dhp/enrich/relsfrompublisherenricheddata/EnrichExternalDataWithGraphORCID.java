
package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import static eu.dnetlib.dhp.PropagationConstant.removeOutputDir;
import static eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE;

import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.common.person.Constants;
import eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans.ResultMatchedSchema;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.rel.CoAuthorship;
import org.apache.commons.collections.ArrayStack;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
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
import scala.Tuple2;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;

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
	// graphPath is the path to the results with information from affro to be enriched
	@Override
	public void createTemporaryData(SparkSession spark, String graphPath, String orcidPath, String targetPath) {
		//the enrichment set is the whole dataset of results computed in the creation of the dataset
		//the pivot is the result id as the deduped record
		//the dataset is computed on the original identifiers: for each folder we need to redirect the result to the merging
		//record.

		//1. Extract from the graph the result - Author information for authors with pids.
		removeOutputDir(spark, targetPath);
		for(EntityType e : ModelSupport.entityTypes.keySet()) {
			if(ModelSupport.isResult(e)){

				Dataset<Row> orcidDnet = spark
						.read()
						.schema(Encoders.bean(Result.class).schema())
						.json(orcidPath + "/" + e.name())
						.as(Encoders.bean(Result.class))
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
						.map((MapFunction<Result, Tuple2<String, OrcidAuthors>>) r -> {
							OrcidAuthors authors = getOrcidAuthorsList(r.getAuthor());
							return new Tuple2<>(r.getId(), authors);
						}, Encoders.tuple(Encoders.STRING(), Encoders.bean(OrcidAuthors.class)))
						.selectExpr("_1 as id", "_2.orcidAuthorList as orcid_authors");// in this case the id is the doi

				orcidDnet.write().mode(SaveMode.Append).option("compression", "gzip").parquet(targetPath + "/graph_authors");
			}
		}


		//2. Selection of the information enriched by affro execution
		Dataset<ResultMatchedSchema> oaire_entities =
				spark.createDataFrame(Collections.emptyList(), Constants.RESULT_MATCHED_SCHEMA)
						.as(Encoders.bean(ResultMatchedSchema.class));

		java.lang.String[] datasources = new java.lang.String[] {
				"oaire", "oalex", "publishers", "crossref", "datacite", "pubmed"
		};
		//If there are matchings for organizations then we have at least one author with a raw affiliation string with a match
		for (String s: datasources){
			oaire_entities = oaire_entities.union(spark.read().schema(Constants.RESULT_MATCHED_SCHEMA). json(graphPath + s)
					.filter(col("organizations").isNotNull()
							.and(size(col("organizations")).gt(0)))
					.as(Encoders.bean(ResultMatchedSchema.class)));
		}

		//3. selection of merges relations to redirect results that have been merged
		//The pivot is the graph id => I need to have consistent identifiers
		Dataset<Relation> mergeRelations = spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(orcidPath + "relation")
				.as(Encoders.bean(Relation.class))
				.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("merges"));

		Dataset<ResultMatchedSchema> oaire_entities_redirected = oaire_entities.joinWith(mergeRelations,
						oaire_entities.col("id").equalTo(mergeRelations.col("target")), "left")
				.map((MapFunction<Tuple2<ResultMatchedSchema, Relation>, ResultMatchedSchema>) t2 -> {
					ResultMatchedSchema rms = t2._1();
					if (t2._2() != null) {
						rms.setId(t2._2().getSource());
					}
					return rms;
				}, Encoders.bean(ResultMatchedSchema.class));


		Dataset<Row> authors = oaire_entities_redirected
			.selectExpr("id", "explode(authors) as author")
			.selectExpr(
				"id",
					"author.fullname as fullname",
				"author.firstname as firstname",
				"author.lastname as lastname",
				"author.pids as pids",
				"author.affiliations as affiliations",
					"author.corresponding as corresponding",
					"author.contributor_roles as roles")
			.map(
				(MapFunction<Row, Tuple2<String, Author>>) a -> new Tuple2<>(a.getAs("is"), getAuthor(a)),
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

		spark.read().parquet(targetPath + "/graph_authors")
			.join(authors, "id")
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.parquet(targetPath + "/publication_unmatched");

	}
//Step2 from the merging record we extract the authors and create an enriched structure containing all
	//the added information we possibly find in the affro enriched records in the graph

	//Step 3 for each eauthor information we group by result id and reconcile the information of all the results enriched
	//for the deduped id
	//example: or1,...orn are merged in dr. suppose we have author affiliation information for or1 and or3. They enrich
	//authorinformation extracted for dr producing the enrichment from or1 and the one from or3. It is possible in these two
	//enrichments the same information is provided, or different information is provided. We need to reconcile and
	//alert if the differences are too big

	//Step4 after reconciliation, new relations for authors are extracted from the update reconciled unique result per oaire id

	private static Dataset<ResultMatchedSchema> getGraphAuthorWithOrcid(SparkSession spark, String orcidPath, EntityType e) {
		return spark.read().schema(Encoders.bean(Result.class).schema())
				.json(orcidPath + e.name())
				.as(Encoders.bean(Result.class))
				.filter((FilterFunction<Result>) r -> !r.getDataInfo().getDeletedbyinference() && !r.getDataInfo().getInvisible())
				.map((MapFunction<Result, ResultMatchedSchema>) r -> {
					if (r.getAuthor().stream().noneMatch(a -> Optional.ofNullable(a.getPid()).isPresent() && !a.getPid().isEmpty()))
						return null;
					ResultMatchedSchema rms = new ResultMatchedSchema();
					rms.setId(r.getId());
					rms.setAuthors(
							r.getAuthor().stream().filter(a -> Optional.ofNullable(a.getPid()).isPresent() && !a.getPid().isEmpty())
									.map(a -> {
										eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans.Author author = new eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans.Author();
										author.setFirstname(a.getName());
										author.setLastname(a.getSurname());
										author.setFullname(a.getFullname());
										author.setPids(a.getPid().stream().map(p -> {
											eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans.Pid pid = new eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans.Pid();
											pid.setSchema(p.getQualifier().getClassid());
											pid.setValue(p.getValue());
											return pid;
										}).collect(Collectors.toList()));
										return author;
									}).collect(Collectors.toList())
					);
					return rms;
				}, Encoders.bean(ResultMatchedSchema.class))
				.filter(Objects::nonNull);
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

		Dataset<CoAuthorship> coAuthorshipRels = graph
			.joinWith(matched, graph.col("id").equalTo(matched.col("id")))
			.flatMap(
				(FlatMapFunction<Tuple2<Row, Row>, CoAuthorship>) EnrichExternalDataWithGraphORCID::coAuthorshipRels,
				Encoders.bean(CoAuthorship.class));

		// need to merge the relations with same source target and semantics
		//TODO adjust this part wrt the new implementation
//		mergeOldAndNewRelations(spark, targetPath, redirectedRels.union(coAuthorshipRels))
//			.write()
//			.mode(SaveMode.Overwrite)
//			.option("compression", "gzip")
//			.json(workingDir + "/relation");

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

	private static Iterator<CoAuthorship> coAuthorshipRels(Tuple2<Row, Row> t2) {

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

		List<CoAuthorship> relList = new ArrayList<>();
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

			List<Row> pidList = pids
				.stream()
				.filter(
					p -> {
						Row qualifier = p.getAs("qualifier");
						Row dataInfo = p.getAs("dataInfo");
						return ModelConstants.ORCID.equalsIgnoreCase(qualifier.getAs("classid"))
							|| ModelConstants.ORCID_PENDING.equalsIgnoreCase(qualifier.getAs("classid"));
					})
				.collect(Collectors.toList());
			pidList
				.forEach(
					p -> relationList
						.add(
							getRelations(
								r.getAs("doi"),
								author.getList(author.fieldIndex("rawAffiliationString")),
								p.getAs("value"))));

		});

		return relationList.iterator();
	}

	private static Relation getRelations(String doi, List<String> rawAffiliationString, String orcid) {
		Relation rel = OafMapperUtils
			.getRelation(
				"30|orcid_______::" + DHPUtils.md5(orcid), "50|doi_________::" + DHPUtils.md5(doi),
				ModelConstants.RESULT_PERSON_RELTYPE, ModelConstants.RESULT_PERSON_SUBRELTYPE,
				ModelConstants.RESULT_PERSON_HASAUTHORED,
				null, DATAINFO, null);
		rawAffiliationString.forEach(raf -> {
			String[] affiliationInfo = raf.split("@@");
			KeyValue kv = new KeyValue();
			if (affiliationInfo[0].equalsIgnoreCase("ror")) {
				kv.setKey("declared_affiliation");
				kv.setValue(affiliationInfo[1]);
				kv
					.setDataInfo(
						OafMapperUtils
							.dataInfo(
								false,
								"openaire:inference",
								true,
								false,
								null,
								affiliationInfo[2]));
			}
			if (!StringUtils.isEmpty(kv.getKey())) {
				if (!Optional.ofNullable(rel.getProperties()).isPresent())
					rel.setProperties(new ArrayList<>());
				rel.getProperties().add(kv);
			}

		});

		return rel;
	}

	private static @NotNull Author getAuthor(Row a) throws JsonProcessingException {
		Author author = new Author();

		author.setName(a.getAs("firstname"));
		author.setFullname(a.getAs("fullname"));
		author.setSurname(a.getAs("lastname"));
		List<StructuredProperty> pids = new ArrayList<>();

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
