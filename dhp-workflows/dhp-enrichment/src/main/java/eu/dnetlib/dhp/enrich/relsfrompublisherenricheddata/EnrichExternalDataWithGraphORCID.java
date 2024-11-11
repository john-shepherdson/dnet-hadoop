package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import com.azul.tooling.in.Model;
import eu.dnetlib.dhp.common.author.SparkEnrichWithOrcidAuthors;
import eu.dnetlib.dhp.orcidtoresultfromsemrel.OrcidAuthors;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.dhp.utils.ORCIDAuthorEnricherResult;
import eu.dnetlib.dhp.utils.OrcidAuthor;
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
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;



public class EnrichExternalDataWithGraphORCID extends SparkEnrichWithOrcidAuthors {
	private static final Logger log = LoggerFactory.getLogger(EnrichExternalDataWithGraphORCID.class);
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
	public EnrichExternalDataWithGraphORCID(String propertyPath, String[] args, Logger log) {
		super(propertyPath, args, log);
	}

	public static void main(String[] args) throws Exception {

		// Create instance and run the Spark application
		EnrichExternalDataWithGraphORCID app = new EnrichExternalDataWithGraphORCID(
			"/eu/dnetlib/dhp/wf/subworkflows/orcidtoresultfromsemrel/input_orcidtoresult_parameters.json", args, log);
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

	@Override
	public void generateGraph(SparkSession spark, String graphPath, String workingDir, String targetPath) {
		//creates new relations
		Dataset<Relation> newRelations = spark.read().schema(Encoders.bean(ORCIDAuthorEnricherResult.class).schema())
				.parquet(workingDir + "/publication_matched")
				.selectExpr("id as doi", "enriched_author")
				.flatMap((FlatMapFunction<Row, Relation>) this::getRelationsList, Encoders.bean(Relation.class));


		//redirects new relations versus representatives if any
		Dataset<Row> graph_relations = spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(graphPath + "/relation")
				.filter("relClass = 'merges'")
				.select("source", "target");

		Dataset<Relation> redirectedRels = newRelations.joinWith(graph_relations, newRelations.col("target").equalTo(graph_relations.col("target")), "left")
				.map((MapFunction<Tuple2<Relation, Row>, Relation>) t2 -> {
					if (t2._2() != null)
						t2._1().setSource(t2._2().getAs("source"));
					return t2._1();
				}, Encoders.bean(Relation.class));

		//need to merge the relations with same source target and semantics
		spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(graphPath + "/relation").as(Encoders.bean(Relation.class))
				.union(redirectedRels)
				.groupByKey((MapFunction<Relation, String>) r -> r.getSource() + r.getRelClass() + r.getTarget(), Encoders.STRING() )
				.mapGroups((MapGroupsFunction<String, Relation, Relation>) (k,it) ->{
					final Relation[] ret = {it.next()};
					it.forEachRemaining(r-> ret[0] = MergeUtils.mergeRelation(ret[0], r));
					return ret[0];
				}, Encoders.bean(Relation.class))
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression","gzip")
				.json(workingDir + "/relation");

		//write the new relations in the relation folder
		spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(workingDir + "/relation")
				.write()
				.option("compression","gzip")
				.mode(SaveMode.Overwrite)
				.json(graphPath + "/relation");


	}

	private Iterator<Relation> getRelationsList(Row r) {
		List<Relation> relationList = new ArrayList<>();
		List<Row> eauthors = r.getAs("enriched_author");

		eauthors.forEach(author -> {
			List<Row> pids =author.getAs("pid");
			pids.forEach(p -> {
				if(p.getAs("scheme")==ModelConstants.ORCID)
					relationList.add(getRelations(r.getAs("doi"),author.getAs("raw_affiliation_string"), p.getAs("value")));
			});
		});
			return relationList.iterator();
    }

	private Relation getRelations(String doi, List<String> rawAffiliationString, String orcid) {
		Relation rel = OafMapperUtils.getRelation("30|orcid_______::"+ DHPUtils.md5(orcid) , "50|doi_________::" + DHPUtils.md5(doi)
				,ModelConstants.RESULT_PERSON_RELTYPE, ModelConstants.RESULT_PERSON_SUBRELTYPE, ModelConstants.RESULT_PERSON_HASAUTHORED,
				null, DATAINFO, null);
		rawAffiliationString.forEach(raf -> {
			String[] affiliationInfo = raf.split("@@");
			KeyValue kv = new KeyValue();
			kv.setKey("declared_affiliation");
			if (affiliationInfo[0].equalsIgnoreCase("ror"))
				kv.setValue(affiliationInfo[1]);
			if (!Optional.ofNullable(rel.getProperties()).isPresent())
				rel.setProperties(new ArrayList<>());
			rel.getProperties().add(kv);

		});

		return rel;
	}


	@Override
	public void createTemporaryData(SparkSession spark, String graphPath, String orcidPath, String targetPath) {
		//Done only for publications since it is the input from the publishers which should be enriched
//creates tuple2 <doi, orcidauthorslist>
				Dataset<Row> orcidDnet = spark
					.read()
					.schema(Encoders.bean(Result.class).schema())
					.json(graphPath + "/publication")
					.as(Encoders.bean(Result.class))
						//selects only publications with doi since it is the only way to match with publisher data
						.filter((FilterFunction<Result>) r-> r.getPid() != null &&
								r.getPid().stream().anyMatch(p->p.getQualifier().getClassid().equalsIgnoreCase("doi")))
						//select only the results with at least the orcid for one author
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
						.flatMap((FlatMapFunction<Result, Tuple2<String,OrcidAuthors>>) r -> {
							List<Tuple2<String,OrcidAuthors>>t2 = new ArrayList<>();
							List<String> dois = r.getPid().stream().filter(p -> p.getQualifier().getClassid().equalsIgnoreCase("doi")).map(p -> p.getValue()).collect(Collectors.toList());
							OrcidAuthors authors = getOrcidAuthorsList(r.getAuthor());
							dois.forEach(doi -> t2.add(new Tuple2<>(doi, authors)));
							return t2.iterator();
						}, Encoders.tuple(Encoders.STRING(), Encoders.bean(OrcidAuthors.class)))
					.selectExpr("_1 as id", "_2.orcidAuthorList as orcid_authors");//in this case the id is the doi

		Dataset<Row> df = spark
				.read()
				.schema(
						"`DOI` STRING, " +
								"`Authors` ARRAY<STRUCT<`Corresponding` : STRING, " +
								"`Contributor_roles` : ARRAY<STRUCT<`Schema`:STRING, `Value`:STRING>> ," +
								"`Name` : STRUCT<`Full`:STRING, `First` : STRING, `Last`: STRING>,  " +
								"`Matchings`: ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>, " +
								"`PIDs` : ARRAY<STRUCT<`Schema`:STRING , `Value`: STRING>>>>")
				.json(graphPath) //the path to the publisher files
				.where("DOI is not null");


		Dataset<Row> authors = df
				.selectExpr("DOI as doi", "explode(Authors) as author")
				.selectExpr(
						"doi", "author.Name.Full as fullname",
						"author.Name.First as firstname",
						"author.Name.Last as lastname",
						"author.PIDs as pids" ,
						"author.Matchings as affiliations")
				.map((MapFunction<Row,Tuple2<String,  Author>> ) a ->
					new Tuple2<>(a.getAs("doi"), getAuthor(a))
		 , Encoders.tuple(Encoders.STRING(), Encoders.bean(Author.class)))
				.groupByKey((MapFunction<Tuple2<String, Author>, String>) t2 -> t2._1() , Encoders.STRING())
				.mapGroups((MapGroupsFunction<String, Tuple2<String, Author>, Tuple2<String,PublisherAuthors>>) (k,it) -> {
					PublisherAuthors pa = new PublisherAuthors();
					while(it.hasNext())
						pa.getPublisherAuthorList().add(it.next()._2());
					return new Tuple2<>(k, pa);
				} , Encoders.tuple(Encoders.STRING(), Encoders.bean(PublisherAuthors.class)))
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

		((List<Row>)a.getAs("pids")).forEach(pid -> pids.add(getPid(pid)));
		//"`Matchings`: ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>,
		((List<Row>)a.getAs("affiliations")).forEach(aff -> {
			if(aff.getAs("Status").equals(Boolean.TRUE))
				affs.add(aff.getAs("PID") + "@@" + aff.getAs("Value") + "@@" + String.valueOf(aff.getAs("Confidence")));

		});

		author.setPid(pids);
		//in this case the rawaffiliation string is used as an accumulator to create relations
		//a little hack not to have to change the schema and /or the implementazion of the analysis method
		author.setRawAffiliationString(affs);
		return author;
	}

	private static @Nullable StructuredProperty getPid(Row pid) {
		return OafMapperUtils.structuredProperty(pid.getAs("Value"),
				OafMapperUtils.qualifier(pid.getAs("Schema"),
						pid.getAs("Schema"),
						ModelConstants.DNET_PID_TYPES,
						ModelConstants.DNET_PID_TYPES),
				null);
	}

}
