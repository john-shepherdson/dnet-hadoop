
package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.PropagationConstant;
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
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import static eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE;

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
//graphPath is the path to the publisher file
	//targetPath is the path to the graph
	@Override
	public void generateGraph(SparkSession spark, String graphPath, String workingDir, String targetPath) {
		// creates new relations of authorship
		Dataset<Relation> newRelations = spark
			.read()
			.schema(Encoders.bean(ORCIDAuthorEnricherResult.class).schema())
			.parquet(workingDir + "/publication_matched")
			.selectExpr("id as doi", "enriched_author")
			.flatMap((FlatMapFunction<Row, Relation>) EnrichExternalDataWithGraphORCID::getRelationsList, Encoders.bean(Relation.class));

		// redirects new relations versus representatives if any
		Dataset<Row> graph_relations = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(targetPath + "/relation")
			.filter("relClass = 'merges'")
			.select("source", "target");
		graph_relations.show(false);
		Dataset<Relation> redirectedRels = newRelations
			.joinWith(graph_relations, newRelations.col("target").equalTo(graph_relations.col("target")), "left")
			.map((MapFunction<Tuple2<Relation, Row>, Relation>) t2 -> {
				if (t2._2() != null)
					t2._1().setSource(t2._2().getAs("source"));
				return t2._1();
			}, Encoders.bean(Relation.class));
		redirectedRels.show(false);
		// need to merge the relations with same source target and semantics
		spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(targetPath + "/relation")
			.as(Encoders.bean(Relation.class))
			.union(redirectedRels)
			.groupByKey(
				(MapFunction<Relation, String>) r -> r.getSource() + r.getRelClass() + r.getTarget(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Relation, Relation>) (k, it) -> {
				final Relation[] ret = {
					it.next()
				};
				it.forEachRemaining(r -> ret[0] = MergeUtils.mergeRelation(ret[0], r));
				return ret[0];
			}, Encoders.bean(Relation.class))
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

	private static Iterator<Relation> getRelationsList(Row r) {

		List<Relation> relationList = new ArrayList<>();

		List<Row> eauthors = JavaConverters.seqAsJavaListConverter(((WrappedArray<Row>) r.getAs("enriched_author")).seq()).asJava();

		eauthors.forEach(author -> {
			List<Row> pids = JavaConverters.seqAsJavaListConverter(((WrappedArray<Row>) author.getAs("pid")).seq()).asJava();
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
						.add(getRelations(r.getAs("doi"), JavaConverters.seqAsJavaListConverter(((WrappedArray<String>)author.getAs("rawAffiliationString")).seq()).asJava(), p.getAs("value"))));
			new CoAuthorshipIterator(extractCoAuthors(pidList)).forEachRemaining(relationList::add);

		});
		return relationList.iterator();
	}

	private static List<String> extractCoAuthors(List<Row> pidList) {

		List<String> coauthors = new ArrayList<>();
		for (Row pid : pidList)
			coauthors.add(pid.getAs("value"));

		return coauthors;
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
			if (affiliationInfo[0].equalsIgnoreCase("ror")){
				kv.setKey("declared_affiliation");
				kv.setValue(affiliationInfo[1]);
				kv.setDataInfo(OafMapperUtils
						.dataInfo(
								false,
								"openaire:inference",
								true,
								false,
								null,
								affiliationInfo[2]));
			}
			if(!StringUtils.isEmpty(kv.getKey())){
				if (!Optional.ofNullable(rel.getProperties()).isPresent())
					rel.setProperties(new ArrayList<>());
				rel.getProperties().add(kv);
			}




		});

		return rel;
	}

//orcidPath is the path to the source for orcid. In this case the oaire graph
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

	private static @NotNull Author getAuthor(Row a) {
		Author author = new Author();

		author.setName(a.getAs("firstname"));
		author.setFullname(a.getAs("fullname"));
		author.setSurname(a.getAs("lastname"));
		List<StructuredProperty> pids = new ArrayList<>();
		List<String> affs = new ArrayList<>();

		List<Row> publisherPids = new ArrayList<>();
		if (Optional.ofNullable(a.getAs("pids")).isPresent())
			publisherPids = JavaConverters.seqAsJavaListConverter(((WrappedArray<Row>) a.getAs("pids")).seq()).asJava();

		publisherPids.forEach(pid -> pids.add(getPid(pid)));

		List<Row> affiliations = JavaConverters
			.seqAsJavaListConverter(((WrappedArray<Row>) a.getAs("affiliations")).seq())
			.asJava();
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
