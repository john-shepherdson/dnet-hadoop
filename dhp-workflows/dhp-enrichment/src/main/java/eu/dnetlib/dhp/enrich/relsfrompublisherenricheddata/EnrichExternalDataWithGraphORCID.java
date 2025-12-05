
package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import static eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE;

import java.util.*;
import java.util.stream.Collectors;

import com.sun.org.apache.xpath.internal.operations.String;
import eu.dnetlib.dhp.common.person.Constants;
import eu.dnetlib.dhp.schema.oaf.rel.CoAuthorship;
import org.apache.commons.collections.ArrayStack;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
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
		//Selection of the information enriched by affro execution
		String[] datasources = {"oaire", "oalex", "publishers", "crossref", "datacite", "pubmed"};

		resultsWithAffiliations = spark.em

		//Step2 from the merging record we extract the authors and create an enriched structure AthorInformation containing all
		//the added information we possibly find in the affro enriched resords in the graph

		//Step 3 for each eauthor information we group by result id and reconcile the information of all the results enriched
		//for the deduped id
		//example: or1,...orn are merged in dr. suppose we have author affiliation information for or1 and or3. They enrich
		//authorinformation extracted for dr producing the enrichment from or1 and the one from or3. It is possible in these two
		//enrichments the same information is provided, or different information is provided. We need to reconcile and
		//alert if the differences are too big

		//Step4 after reconciliation, new relations for authors are extracted from the update reconciled unique result per oaire id

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
		StructType schema = new StructType()
			.add("DOI", DataTypes.StringType)
			.add(
				"Authors", DataTypes
					.createArrayType(
						new StructType()
							.add("Corresponding", DataTypes.StringType)
							.add(
								"Contributor_roles", DataTypes
									.createArrayType(
										new StructType()
											.add("Schema", DataTypes.StringType)
											.add("Value", DataTypes.StringType)))
							.add(
								"Name", new StructType()
									.add("Full", DataTypes.StringType)
									.add("First", DataTypes.StringType)
									.add("Last", DataTypes.StringType))
							.add(
								"Matchings", DataTypes
									.createArrayType(
										new StructType()
											.add("PID", DataTypes.StringType)
											.add("Value", DataTypes.StringType)
											.add("Confidence", DataTypes.DoubleType)
											.add("Status", DataTypes.StringType)))
							.add(
								"PIDs", DataTypes
									.createArrayType(
										new StructType()
											.add("Schema", DataTypes.StringType)
											.add("Value", DataTypes.StringType)))));

		Dataset<Row> df = spark
			.read()
			.schema(schema)
			.json(graphPath) // the path to the publisher files
			.where("DOI is not null");

		Dataset<Row> authors = df
			.selectExpr("DOI as doi", "explode(Authors) as author")
			.selectExpr(
				"doi", "author.Name.Full as fullname",
				"author.Name.First as firstname",
				"author.Name.Last as lastname",
				"author.PIDs as pids",
				"author.Matchings as affiliations")
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

	private static @NotNull Author getAuthor(Row a) {
		Author author = new Author();

		author.setName(a.getAs("firstname"));
		author.setFullname(a.getAs("fullname"));
		author.setSurname(a.getAs("lastname"));
		List<StructuredProperty> pids = new ArrayList<>();
		List<String> affs = new ArrayList<>();

		List<Row> publisherPids = new ArrayList<>();
		if (Optional.ofNullable(a.getAs("pids")).isPresent())
			publisherPids = a.getList(a.fieldIndex("pids"));

		publisherPids.forEach(pid -> pids.add(getPid(pid)));

		List<Row> affiliations = a.getList(a.fieldIndex("affiliations"));
		// "`Matchings`: ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>,
		affiliations.forEach(aff -> {
			String pidtype = aff.getAs("PID");
			String pidvalue = aff.getAs("Value");
			Double pidconfidence = aff.getAs("Confidence");

			if (aff.getAs("Status").equals("active"))
				affs.add(pidtype + "@@" + pidvalue + "@@" + pidconfidence);

		});

		author.setPid(pids);
		// in this case the rawaffiliation string is used as an accumulator to create relations
		// a little hack not to have to change the schema and /or the implementazion of the analysis method
		author.setRawAffiliationString(affs);
		return author;
	}

	private static @Nullable StructuredProperty getPid(Row pid) {
		return OafMapperUtils
			.structuredProperty(
				pid.getAs("Value"),
				OafMapperUtils
					.qualifier(
						pid.getAs("Schema"),
						pid.getAs("Schema"),
						ModelConstants.DNET_PID_TYPES,
						ModelConstants.DNET_PID_TYPES),
				null);
	}

}
