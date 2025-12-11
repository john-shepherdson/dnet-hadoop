
package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import static eu.dnetlib.dhp.PropagationConstant.getRelation;
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
import eu.dnetlib.dhp.schema.oaf.rel.Authorship;
import eu.dnetlib.dhp.schema.oaf.rel.CoAuthorship;
import eu.dnetlib.dhp.schema.oaf.rel.beans.AuthorshipRoles;
import eu.dnetlib.dhp.schema.oaf.rel.beans.DeclaredAffiliation;
import eu.dnetlib.dhp.schema.oaf.rel.beans.MatchingOrganization;
import eu.dnetlib.dhp.schema.oaf.rel.beans.Role;
import org.apache.commons.lang3.StringUtils;
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
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.ORCIDAuthorEnricherResult;
import eu.dnetlib.dhp.utils.OrcidAuthor;
import scala.Tuple2;

import static eu.dnetlib.dhp.common.person.Constants.OPENAIRE_DATAINFO;
import static eu.dnetlib.dhp.common.person.Constants.RESULT_MATCHED_SCHEMA;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.expr;

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

	public static final String OPENAIRE_DATASOURCE_ID = "10|infrastruct_::f66f1bd369679b5b077dcdf006089556";
	public static final String OPENAIRE_DATASOURCE_NAME = "OpenAIRE";
	public static final List<KeyValue> OPENAIRE_COLLECTED_FROM = OafMapperUtils
			.listKeyValues(OPENAIRE_DATASOURCE_ID, OPENAIRE_DATASOURCE_NAME);

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
						.selectExpr("_1 as id", "_2.orcidAuthorList as orcid_authors");

				orcidDnet.write().mode(SaveMode.Append).option("compression", "gzip").parquet(targetPath + "/graph_authors");
			}
		}


		//2. Selection of the information enriched by affro execution
		Dataset<ResultMatchedSchema> oaire_entities =
				spark.createDataFrame(Collections.emptyList(), Encoders.bean(ResultMatchedSchema.class).schema())
						.as(Encoders.bean(ResultMatchedSchema.class));

		java.lang.String[] datasources = new java.lang.String[] {
				"oaire", "oalex", "publishers", "crossref", "datacite", "pubmed"
		};
		//If there are matchings for organizations then we have at least one author with a raw affiliation string with a match
		for (String s: datasources){
			oaire_entities = oaire_entities.union(spark.read().schema(Encoders.bean(ResultMatchedSchema.class).schema())
					. json(graphPath + s)
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

		spark.read().parquet(targetPath + "/graph_authors")
			.join(authors, "id")
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.parquet(targetPath + "/publication_unmatched");

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
								so.setRaw(aff.getAs("raw_affiliation_string"));
								so.setMatchings(aff.getList(aff.fieldIndex("matchings"))
										.stream().map( m -> {
											Row matching = (Row)m;
											SerializationMatching sm = new SerializationMatching();
											if("ror".equalsIgnoreCase(matching.getAs("pid")))
												sm.setRor(matching.getAs("value"));
											else
												sm.setOpenOrgs(matching.getAs("value"));
											sm.setConfidence(matching.getAs("confidence"));
											sm.setName(matching.getAs("name"));
											sm.setCountry(matching.getAs("country"));
											return sm;
										}).collect(Collectors.toList()));


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

	//Step2 from the merging record we extract the authors and create an enriched structure containing all
	//the added information we possibly find in the affro enriched records in the graph

	//Step 3 for each eauthor information we group by result id and reconcile the information of all the results enriched
	//for the deduped id
	//example: or1,...orn are merged in dr. suppose we have author affiliation information for or1 and or3. They enrich
	//authorinformation extracted for dr producing the enrichment from or1 and the one from or3. It is possible in these two
	//enrichments the same information is provided, or different information is provided. We need to reconcile and
	//alert if the differences are too big

	//Step4 after reconciliation, new relations for authors are extracted from the update reconciled unique result per oaire id


	// graphPath is the path to the publisher file
	// targetPath is the path to the graph
	@Override
	public void generateGraph(SparkSession spark, String graphPath, String workingDir, String targetPath) {

		// creates new relations of authorship with the declared_affiliation property
		// or without declared affiliation

		spark
				.read()
				.schema(Encoders.bean(ORCIDAuthorEnricherResult.class).schema())
				.parquet(workingDir + "/publication_matched")
				.selectExpr("id", "explode(enriched_author) as eauthor")//, "orcid_unmatched")
				.map((MapFunction<Row, Authorship>)  EnrichExternalDataWithGraphORCID::getAuthorshipRelation,
						Encoders.bean(Authorship.class))
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(workingDir + "/authorship");

		//TODO create the authorship relations for the unmatched orcid as in the new model
		spark
				.read()
				.schema(Encoders.bean(ORCIDAuthorEnricherResult.class).schema())
				.parquet(workingDir + "/publication_matched")
				.selectExpr("id", "explode(orcid_unmatched) as uauthor")
				.map((MapFunction<Row, Authorship>)  EnrichExternalDataWithGraphORCID::getAuthorshipRelation,
						Encoders.bean(Authorship.class))
				.write()
				.mode(SaveMode.Append)
				.option("compression","gzip")
				.json(workingDir + "/authorship");


		// create co authorship relations directly from the graph
		Dataset<Row> graph = spark.read().parquet(workingDir + "/graph_authors");


		graph
			.flatMap(
				(FlatMapFunction<Row, CoAuthorship>) EnrichExternalDataWithGraphORCID::coAuthorshipRels,
				Encoders.bean(CoAuthorship.class))
				//now I have to merge the co-authorship relations
				.groupByKey((MapFunction<CoAuthorship, String>) c -> c.getAuthor1() + "::" + c.getAuthor2(), Encoders.STRING())
				.mapGroups((MapGroupsFunction<String, CoAuthorship, CoAuthorship>) (k,it) ->  {
							CoAuthorship ca = it.next();
							ca.setCoauthoredProducts(1);
							it.forEachRemaining(entry -> ca.setCoauthoredProducts(ca.getCoauthoredProducts() + 1));
							return ca;
						}

				, Encoders.bean(CoAuthorship.class))
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(workingDir  + "/coAuthorship");


		//this is the only place where we create relations of authorship and co-authorship for person.

		// write the new relations in the relation folder
		spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(workingDir + "/authorship")
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(targetPath + "/authorship");

		spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(workingDir + "/coAuthorship")
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(targetPath + "/coAuthorship");

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

	private static Iterator<CoAuthorship> coAuthorshipRels(Row t2) {

		List<String> authorsList1 = t2

			.<Row> getList(t2.fieldIndex("orcid_authors"))
			.stream()
			.map(a -> (String) a.getAs("orcid"))
			.collect(Collectors.toList());

		List<CoAuthorship> relList = new ArrayList<>();
		new CoAuthorshipIterator(authorsList1).forEachRemaining(r -> relList.add(r));
		return relList.iterator();

	}

	private static Dataset<Relation> getNewRelations(SparkSession spark, String workingDir) {
		return spark
			.read()
			.schema(Encoders.bean(ORCIDAuthorEnricherResult.class).schema())
			.parquet(workingDir + "/publication_matched")
			.selectExpr("id", "enriched_author", "orcid_unmatched")
			.flatMap(
				(FlatMapFunction<Row, Relation>) EnrichExternalDataWithGraphORCID::getRelationsList,
				Encoders.bean(Relation.class));
	}

	private static Iterator<Relation> getRelationsList(Row r) {

		List<Relation> relationList = new ArrayList<>();

		List<Row> orcidForRelation = r.getList(r.fieldIndex("enriched_author"));

		orcidForRelation.forEach(author -> {
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
								p.getAs("value"))));

		});
		orcidForRelation = r.getList(r.fieldIndex("orcid_unmatched"));
		orcidForRelation.forEach(orcidUnmatched -> {
			String orcid = orcidUnmatched.getAs("orcid");
			relationList.add(getRelations(r.getAs("id"), null, orcid ));

		});
		return relationList.iterator();
	}



	private static Authorship getAuthorshipRelation(Row row) throws JsonProcessingException {

		String id = row.getAs("id");
		Authorship authorship = new Authorship();
		authorship.setProduct(id);
		authorship.setCollectedfrom(OPENAIRE_COLLECTED_FROM);
		authorship.setDataInfo(OPENAIRE_DATAINFO);
		List<Row> pids = row.getList(row.fieldIndex("pid"));
		String orcid = null;
		if(pids != null) {

			pids
					.stream()
					.filter(
							p -> {
								Row qualifier = p.getAs("qualifier");
								return ModelConstants.ORCID.equalsIgnoreCase(qualifier.getAs("classid"));
							})
					.collect(Collectors.toList());
			if (!pids.isEmpty())
				if (pids.stream().anyMatch(p -> {
					Row qualifier = p.getAs("qualifier");
					return ModelConstants.ORCID.equalsIgnoreCase(qualifier.getAs("classid"));
				}))
					orcid = pids.stream().filter(p -> {
								Row qualifier = p.getAs("qualifier");
								return ModelConstants.ORCID.equalsIgnoreCase(qualifier.getAs("classid"));
							})
							.collect(Collectors.toList()).get(0).getAs("value");
				else
					orcid = pids.stream().filter(p -> {
								Row qualifier = p.getAs("qualifier");
								return ModelConstants.ORCID_PENDING.equalsIgnoreCase(qualifier.getAs("classid"));
							})
							.collect(Collectors.toList()).get(0).getAs("value");
		}
		else {
			orcid = row.getAs("orcid");
		}
			authorship.setPerson(Constants.getPersonId(orcid));


			List<String> rawAffiliationString = row.getList(row.fieldIndex("rawAffiliationString"));
			if (rawAffiliationString != null) {
				SerializationBean sb = new ObjectMapper().readValue(rawAffiliationString.get(0), SerializationBean.class);
				if (sb.getCorresponding() != null)
					authorship.setCorresponding(sb.getCorresponding());
				if (sb.getRoles() != null && !sb.getRoles().isEmpty()) {
					List<Role> roles = new ArrayList<>();
					sb.getRoles().forEach(role -> {
						Role authorRole = new Role();
						String schema = role.getRoleSchema();
						if (StringUtils.isNotBlank(schema) && "Credit".equalsIgnoreCase(schema)) {
							AuthorshipRoles r = AuthorshipRoles.fromString(role.getRoleValue());
							if (r != null) {
								authorRole.setRole(r);
								authorRole.setValue(role.getRoleValue());
								authorRole.setSchema(schema);
							}
						}
						authorRole.setText(role.getRoleName());

						roles.add(authorRole);
					});
					authorship.setRoles(roles);
				}
				if (sb.getAffs() != null && !sb.getAffs().isEmpty()) {
					authorship.setDeclaredAffiliations(sb.getAffs().stream().map(aff -> {
						DeclaredAffiliation da = new DeclaredAffiliation();
						da.setRawAffiliation(aff.getRaw());
						da.setMatchingOrganization(aff.getMatchings().stream().map(
								o -> {
									MatchingOrganization mo = new MatchingOrganization();
									mo.setCountry(o.getCountry());
									mo.setTrust(o.getConfidence());
									mo.setOpenOrgs(o.getOpenOrgs());
									mo.setRor(o.getRor());
									mo.setResolvedOrganizationName(o.getName());
									mo.setProvenance("affro");
									return mo;
								}
						).collect(Collectors.toList()));
						return da;
					}).collect(Collectors.toList()));


				}

			}

		return authorship;
	}


	private static Relation getRelations(String resultId, List<String> rawAffiliationString, String orcid) {
		Relation rel = OafMapperUtils
			.getRelation(
				Constants.getPersonId(orcid), resultId,
				ModelConstants.RESULT_PERSON_RELTYPE, ModelConstants.RESULT_PERSON_SUBRELTYPE,
				ModelConstants.RESULT_PERSON_HASAUTHORED,
				null, DATAINFO, null);
		if (rawAffiliationString != null) {
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
		}
		return rel;
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
