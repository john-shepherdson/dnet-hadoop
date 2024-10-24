
package eu.dnetlib.dhp.actionmanager.personentity;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.common.person.CoAuthorshipIterator;
import eu.dnetlib.dhp.common.person.Coauthors;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.jetty.util.StringUtil;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.orcid.model.Author;
import eu.dnetlib.dhp.collection.orcid.model.Employment;
import eu.dnetlib.dhp.collection.orcid.model.Work;
import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Person;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

public class ExtractPerson implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(ExtractPerson.class);
	private static final String QUERY = "SELECT * FROM project_person WHERE pid_type = 'ORCID'";
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String OPENAIRE_PREFIX = "openaire____";
	private static final String SEPARATOR = "::";
	private static final String orcidKey = "10|" + OPENAIRE_PREFIX + SEPARATOR
		+ DHPUtils.md5(ModelConstants.ORCID.toLowerCase());

	private static final String DOI_PREFIX = "50|doi_________::";

	private static final String PMID_PREFIX = "50|pmid________::";
	private static final String ARXIV_PREFIX = "50|arXiv_______::";

	private static final String PMCID_PREFIX = "50|pmcid_______::";
	private static final String ROR_PREFIX = "20|ror_________::";
	private static final String PERSON_PREFIX = ModelSupport.getIdPrefix(Person.class) + "|orcid_______";
	public static final String ORCID_AUTHORS_CLASSID = "sysimport:crosswalk:orcid";
	public static final String ORCID_AUTHORS_CLASSNAME = "Imported from ORCID";
	public static final String FUNDER_AUTHORS_CLASSID = "sysimport:crosswalk:funderdatabase";
	public static final String FUNDER_AUTHORS_CLASSNAME = "Imported from Funder Database";
	public static final String OPENAIRE_DATASOURCE_ID = "10|infrastruct_::f66f1bd369679b5b077dcdf006089556";
	public static final String OPENAIRE_DATASOURCE_NAME = "OpenAIRE";

	public static List<KeyValue> collectedfromOpenAIRE = OafMapperUtils
		.listKeyValues(OPENAIRE_DATASOURCE_ID, OPENAIRE_DATASOURCE_NAME);

	public static final DataInfo ORCIDDATAINFO = OafMapperUtils
		.dataInfo(
			false,
			null,
			false,
			false,
			OafMapperUtils
				.qualifier(
					ORCID_AUTHORS_CLASSID,
					ORCID_AUTHORS_CLASSNAME,
					ModelConstants.DNET_PROVENANCE_ACTIONS,
					ModelConstants.DNET_PROVENANCE_ACTIONS),
			"0.91");

	public static final DataInfo FUNDERDATAINFO = OafMapperUtils
			.dataInfo(
					false,
					null,
					false,
					false,
					OafMapperUtils
							.qualifier(
									FUNDER_AUTHORS_CLASSID,
									FUNDER_AUTHORS_CLASSNAME,
									ModelConstants.DNET_PROVENANCE_ACTIONS,
									ModelConstants.DNET_PROVENANCE_ACTIONS),
					"0.91");

	public static void main(final String[] args) throws IOException, ParseException {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							ExtractPerson.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/actionmanager/personentity/as_parameters.json"))));

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}", outputPath);

		final String workingDir = parser.get("workingDir");
		log.info("workingDir {}", workingDir);

		final String dbUrl = parser.get("postgresUrl");
		final String dbUser = parser.get("postgresUser");
		final String dbPassword = parser.get("postgresPassword");

		final String hdfsNameNode = parser.get("hdfsNameNode");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());
				extractInfoForActionSetFromORCID(spark, inputPath, workingDir);
				extractInfoForActionSetFromProjects(
					spark, inputPath, workingDir, dbUrl, dbUser, dbPassword, workingDir + "/project", hdfsNameNode);
				createActionSet(spark, outputPath, workingDir);
			});

	}

	private static void extractInfoForActionSetFromProjects(SparkSession spark, String inputPath, String workingDir,
		String dbUrl, String dbUser, String dbPassword, String hdfsPath, String hdfsNameNode) throws IOException {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);
		Path hdfsWritePath = new Path(hdfsPath);
		FSDataOutputStream fos = fileSystem.create(hdfsWritePath);
		try (DbClient dbClient = new DbClient(dbUrl, dbUser, dbPassword)) {
			try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8))) {
				dbClient.processResults(QUERY, rs -> writeRelation(getRelationWithProject(rs), writer));
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static Relation getRelationWithProject(ResultSet rs) {
		try {
			return getProjectRelation(
				rs.getString("project"), rs.getString("pid"),
				rs.getString("role"));
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static Relation getProjectRelation(String project, String orcid, String role) {

		String source = PERSON_PREFIX + "::" + IdentifierFactory.md5(orcid);
		String target = project.substring(0, 14)
			+ IdentifierFactory.md5(project.substring(15));
		List<KeyValue> properties = new ArrayList<>();

		Relation relation = OafMapperUtils
			.getRelation(
				source, target, ModelConstants.PROJECT_PERSON_RELTYPE, ModelConstants.PROJECT_PERSON_SUBRELTYPE,
				ModelConstants.PROJECT_PERSON_PARTICIPATES,
				collectedfromOpenAIRE,
					FUNDERDATAINFO,
				null);
		relation.setValidated(true);

		if (StringUtil.isNotBlank(role)) {
			KeyValue kv = new KeyValue();
			kv.setKey("role");
			kv.setValue(role);
			properties.add(kv);
		}

		if (!properties.isEmpty())
			relation.setProperties(properties);
		return relation;

	}

	protected static void writeRelation(final Relation relation, BufferedWriter writer) {
		try {
			writer.write(OBJECT_MAPPER.writeValueAsString(relation));
			writer.newLine();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void createActionSet(SparkSession spark, String outputPath, String workingDir) {

		Dataset<Person> people;
		people = spark
			.read()
			.textFile(workingDir + "/people")
			.map(
				(MapFunction<String, Person>) value -> OBJECT_MAPPER
					.readValue(value, Person.class),
				Encoders.bean(Person.class));

		people
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.union(
				getRelations(spark, workingDir + "/authorship").toJavaRDD().map(r -> new AtomicAction(r.getClass(), r)))
			.union(
				getRelations(spark, workingDir + "/coauthorship")
					.toJavaRDD()
					.map(r -> new AtomicAction(r.getClass(), r)))
			.union(
				getRelations(spark, workingDir + "/affiliation")
					.toJavaRDD()
					.map(r -> new AtomicAction(r.getClass(), r)))
			.union(
				getRelations(spark, workingDir + "/project")
					.toJavaRDD()
					.map(r -> new AtomicAction(r.getClass(), r)))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(
				outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, BZip2Codec.class);
	}

	private static void extractInfoForActionSetFromORCID(SparkSession spark, String inputPath, String workingDir) {
		Dataset<Author> authors = spark
			.read()
			.parquet(inputPath + "Authors")
			.as(Encoders.bean(Author.class));

		Dataset<Work> works = spark
			.read()
			.parquet(inputPath + "Works")
			.as(Encoders.bean(Work.class))
			.filter(
				(FilterFunction<Work>) w -> Optional.ofNullable(w.getPids()).isPresent() &&
					w
						.getPids()
						.stream()
						.anyMatch(
							p -> p.getSchema().equalsIgnoreCase("doi") ||
								p.getSchema().equalsIgnoreCase("pmc") ||
								p.getSchema().equalsIgnoreCase("pmid") ||
								p.getSchema().equalsIgnoreCase("arxiv")));

		Dataset<Employment> employmentDataset = spark
			.read()
			.parquet(inputPath + "Employments")
			.as(Encoders.bean(Employment.class));

		Dataset<Employment> employment = employmentDataset
			.joinWith(authors, employmentDataset.col("orcid").equalTo(authors.col("orcid")))
			.map((MapFunction<Tuple2<Employment, Author>, Employment>) t2 -> t2._1(), Encoders.bean(Employment.class));

		// Mapping all the orcid profiles even if the profile has no visible works

		authors.map((MapFunction<Author, Person>) op -> {
			Person person = new Person();
			person.setId(DHPUtils.generateIdentifier(op.getOrcid(), PERSON_PREFIX));
			person
				.setBiography(
					Optional
						.ofNullable(op.getBiography())

						.orElse(""));
			KeyValue kv = OafMapperUtils.keyValue(orcidKey, ModelConstants.ORCID_DS);
			kv.setDataInfo(null);
			person.setCollectedfrom(Arrays.asList(kv));
			person
				.setAlternativeNames(
					Optional
						.ofNullable(op.getOtherNames())

						.orElse(new ArrayList<>()));
			person
				.setFamilyName(
					Optional
						.ofNullable(op.getFamilyName())

						.orElse(""));
			person
				.setGivenName(
					Optional
						.ofNullable(op.getGivenName())

						.orElse(""));
			person
				.setPid(
					Optional
						.ofNullable(op.getOtherPids())
						.map(
							v -> v
								.stream()
								.map(
									p -> OafMapperUtils
										.structuredProperty(
											p.getValue(), p.getSchema(), p.getSchema(), ModelConstants.DNET_PID_TYPES,
											ModelConstants.DNET_PID_TYPES, null))
								.collect(Collectors.toList()))
						.orElse(new ArrayList<>()));
			person
				.getPid()
				.add(
					OafMapperUtils
						.structuredProperty(
							op.getOrcid(), ModelConstants.ORCID, ModelConstants.ORCID_CLASSNAME,
							ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES, null));
			person.setDateofcollection(op.getLastModifiedDate());
			person.setOriginalId(Arrays.asList(op.getOrcid()));
			person.setDataInfo(ORCIDDATAINFO);
			return person;
		}, Encoders.bean(Person.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(workingDir + "/people");

		works
			.flatMap(
				(FlatMapFunction<Work, Relation>) ExtractPerson::getAuthorshipRelationIterator,
				Encoders.bean(Relation.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(workingDir + "/authorship");

		Dataset<Relation> coauthorship = works
			.flatMap((FlatMapFunction<Work, Tuple2<String, String>>) w -> {
				List<Tuple2<String, String>> lista = new ArrayList<>();
				w.getPids().stream().forEach(p -> {
					if (p.getSchema().equalsIgnoreCase("doi") || p.getSchema().equalsIgnoreCase("pmc")
						|| p.getSchema().equalsIgnoreCase("pmid") || p.getSchema().equalsIgnoreCase("arxiv"))
						lista.add(new Tuple2<>(p.getValue(), w.getOrcid()));
				});
				return lista.iterator();
			}, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
			.groupByKey((MapFunction<Tuple2<String, String>, String>) Tuple2::_1, Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Tuple2<String, String>, Coauthors>) (k, it) -> extractCoAuthors(it),
				Encoders.bean(Coauthors.class))
			.flatMap(
				(FlatMapFunction<Coauthors, Relation>) c -> new CoAuthorshipIterator(c.getCoauthors()),
				Encoders.bean(Relation.class))
			.groupByKey((MapFunction<Relation, String>) r -> r.getSource() + r.getTarget(), Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Relation, Relation>) (k, it) -> it.next(), Encoders.bean(Relation.class));

		coauthorship
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(workingDir + "/coauthorship");

		employment
			.filter((FilterFunction<Employment>) e -> Optional.ofNullable(e.getAffiliationId()).isPresent())
			.filter((FilterFunction<Employment>) e -> e.getAffiliationId().getSchema().equalsIgnoreCase("ror"))
			.map(
				(MapFunction<Employment, Relation>) ExtractPerson::getAffiliationRelation,
				Encoders.bean(Relation.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(workingDir + "/affiliation");
	}

	private static Dataset<Relation> getRelations(SparkSession spark, String path) {
		return spark
			.read()
			.textFile(path)
			.map(
				(MapFunction<String, Relation>) value -> OBJECT_MAPPER
					.readValue(value, Relation.class),
				Encoders.bean(Relation.class));// spark.read().json(path).as(Encoders.bean(Relation.class));
	}

	private static Coauthors extractCoAuthors(Iterator<Tuple2<String, String>> it) {
		Coauthors coauth = new Coauthors();
		List<String> coauthors = new ArrayList<>();
		while (it.hasNext())
			coauthors.add(it.next()._2());
		coauth.setCoauthors(coauthors);

		return coauth;
	}

	private static Relation getAffiliationRelation(Employment row) {
		String source = PERSON_PREFIX + "::" + IdentifierFactory.md5(row.getOrcid());
		String target = ROR_PREFIX
			+ IdentifierFactory.md5(PidCleaner.normalizePidValue("ROR", row.getAffiliationId().getValue()));
		List<KeyValue> properties = new ArrayList<>();

		Relation relation = OafMapperUtils
			.getRelation(
				source, target, ModelConstants.ORG_PERSON_RELTYPE, ModelConstants.ORG_PERSON_SUBRELTYPE,
				ModelConstants.ORG_PERSON_PARTICIPATES,
				Arrays.asList(OafMapperUtils.keyValue(orcidKey, ModelConstants.ORCID_DS)),
					ORCIDDATAINFO,
				null);
		relation.setValidated(true);

		if (Optional.ofNullable(row.getStartDate()).isPresent() && StringUtil.isNotBlank(row.getStartDate())) {
			KeyValue kv = new KeyValue();
			kv.setKey("startDate");
			kv.setValue(row.getStartDate());
			properties.add(kv);
		}
		if (Optional.ofNullable(row.getEndDate()).isPresent() && StringUtil.isNotBlank(row.getEndDate())) {
			KeyValue kv = new KeyValue();
			kv.setKey("endDate");
			kv.setValue(row.getEndDate());
			properties.add(kv);
		}

		if (properties.size() > 0)
			relation.setProperties(properties);
		return relation;

	}

	private static @NotNull Iterator<Relation> getAuthorshipRelationIterator(Work w) {

		if (Optional.ofNullable(w.getPids()).isPresent())
			return w
				.getPids()
				.stream()
				.map(pid -> getRelation(w.getOrcid(), pid))
				.filter(Objects::nonNull)
				.collect(Collectors.toList())
				.iterator();
		List<Relation> ret = new ArrayList<>();
		return ret.iterator();
	}

	private static Relation getRelation(String orcid, eu.dnetlib.dhp.collection.orcid.model.Pid pid) {
		String target;
		String source = PERSON_PREFIX + "::" + IdentifierFactory.md5(orcid);
		switch (pid.getSchema()) {
			case "doi":
				target = DOI_PREFIX
					+ IdentifierFactory
						.md5(PidCleaner.normalizePidValue(PidType.doi.toString(), pid.getValue()));
				break;
			case "pmid":
				target = PMID_PREFIX
					+ IdentifierFactory
						.md5(PidCleaner.normalizePidValue(PidType.pmid.toString(), pid.getValue()));
				break;
			case "arxiv":
				target = ARXIV_PREFIX
					+ IdentifierFactory
						.md5(PidCleaner.normalizePidValue(PidType.arXiv.toString(), pid.getValue()));
				break;
			case "pmcid":
				target = PMCID_PREFIX
					+ IdentifierFactory
						.md5(PidCleaner.normalizePidValue(PidType.pmc.toString(), pid.getValue()));
				break;

			default:
				return null;
		}
		Relation relation = OafMapperUtils
			.getRelation(
				source, target, ModelConstants.RESULT_PERSON_RELTYPE,
				ModelConstants.RESULT_PERSON_SUBRELTYPE,
				ModelConstants.RESULT_PERSON_HASAUTHORED,
				Arrays.asList(OafMapperUtils.keyValue(orcidKey, ModelConstants.ORCID_DS)),
					ORCIDDATAINFO,
				null);
		relation.setValidated(true);
		return relation;
	}
}
