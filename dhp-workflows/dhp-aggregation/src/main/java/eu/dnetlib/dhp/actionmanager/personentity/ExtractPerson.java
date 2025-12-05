
package eu.dnetlib.dhp.actionmanager.personentity;

import static eu.dnetlib.dhp.actionmanager.personentity.ASConstants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.common.person.Constants.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.common.person.Constants;
import eu.dnetlib.dhp.schema.oaf.rel.AuthorAffiliation;
import eu.dnetlib.dhp.schema.oaf.rel.Authorship;
import eu.dnetlib.dhp.schema.oaf.rel.CoAuthorship;
import eu.dnetlib.dhp.schema.oaf.rel.ProjectParticipation;
import eu.dnetlib.dhp.schema.oaf.rel.beans.*;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.spark.sql.Dataset;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.orcid.model.Author;
import eu.dnetlib.dhp.collection.orcid.model.Employment;
import eu.dnetlib.dhp.collection.orcid.model.Work;
import eu.dnetlib.dhp.common.DbClient;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.person.CoAuthorshipIterator;
import eu.dnetlib.dhp.common.person.Coauthors;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.*;
import scala.Tuple2;
import scala.collection.mutable.WrappedArray;

import static org.apache.spark.sql.functions.*;

public class ExtractPerson implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(ExtractPerson.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

		final String publisherInputPath = parser.get("publisherInputPath");
		log.info("publisherInputPath {}", publisherInputPath);

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
					dbUrl, dbUser, dbPassword, workingDir + "/project", hdfsNameNode, isSparkSessionManaged);
				//extractInfoForActionSetFromPublisher(spark, publisherInputPath, workingDir);
				createActionSet(spark, outputPath, workingDir);
			});

	}

	// PUBLISHER
	private static void extractInfoForActionSetFromPublisher(SparkSession spark, String inputPath, String workingDir) {

		spark
				.udf().register(
						"removeLeadingOrcidUrl", (String pid) -> StringUtils.startsWith(pid, "https") ? StringUtils.substringAfter(pid, "orcid.org/") : pid, DataTypes.StringType);

		// Read the publishers output
		Dataset<Row> df = spark
			.read()
			.schema(RESULT_MATCHED_SCHEMA)

			.json(inputPath)
			.where("doi is not null");

        //Select the relevant information
		Dataset<Row> allAuthors = df
				.withColumn("author", explode(col("authors")))
				.select(col("doi"),
						col("author.contributor_roles").as("roles"),
						col("author.corresponding").as("corresponding"),
						col("author.affiliations").as("affiliations"),
						col("author.pids").as("pids"))
				.withColumn("pid", explode(col("pids")))
				.drop("pids")
				.filter(lower(col("pid.schema")).equalTo("orcid"))
				.withColumn("orcid", expr("removeLeadingOrcidUrl(pid.value)"))
				.drop("pid");

		writeAuthorshipRelations(workingDir + "/authorship", allAuthors);



	}


	private static void writeAuthorshipRelations(String outputPath, Dataset<Row> allAuthors) {

		allAuthors.map(
				(MapFunction<Row, Authorship>) row -> {
					String id = row.getAs("doi");
					Boolean corresponding = Boolean.valueOf(row.getAs("corresponding"));

					String orcid = Constants.getPersonId(row.getAs("orcid"));

					// --- Gestione Affiliazioni (senza explode) ---
					WrappedArray<Row> affRows = row.getAs("affiliations");
					List<DeclaredAffiliation> declaredAffiliations = new ArrayList<>();
					Set<String> insertedPids = new HashSet<>();
					if (affRows != null) {
						for(int i =0; i< affRows.length(); i++){
							Row aff = affRows.apply(i);
							String rawAff = aff.getAs("raw_affiliation_string");

							// Matchings non esplosi
							WrappedArray<Row> matchingRows = aff.getAs("matchings");
							List<MatchingOrganization> mos = new ArrayList<>();

							if (matchingRows != null) {
								for (int j = 0 ; j < matchingRows.length(); j++){
									Row m = matchingRows.apply(j);
									String status = m.getAs("status");
									String pidValue = m.getAs("value");
									if ("active".equalsIgnoreCase(status) && !insertedPids.contains(pidValue)) {
										insertedPids.add(m.getAs("value"));
										MatchingOrganization mo = new MatchingOrganization();
										if("ROR".equalsIgnoreCase(m.getAs("pid"))){
											mo.setRor(pidValue);
										}else {
											mo.setOpenOrgs(m.getAs("value"));
										}
										mo.setProvenance("affro");
										mo.setCountry(m.getAs("country"));
										mo.setTrust(m.getAs("confidence"));
										mo.setResolvedOrganizationName(m.getAs("name"));

										mos.add(mo);
									}
								}
							}

							if (!mos.isEmpty()) {
								DeclaredAffiliation da = new DeclaredAffiliation();
								da.setRawAffiliation(rawAff);
								da.setMatchingOrganization(mos);
								declaredAffiliations.add(da);
							}
						}
					}

					// --- Gestione Roles (senza explode) ---
					WrappedArray<Row> roleRows = row.getAs("roles");
					List<Role> roles = new ArrayList<>();
					if (roleRows != null) {
						for(int i = 0; i < roleRows.length(); i++){
							Row role = roleRows.apply(i);
							Role authorRole = new Role();
							String schema = role.getAs("schema");
							if(StringUtils.isNotBlank(schema) && "Credit".equalsIgnoreCase(schema)){
								AuthorshipRoles r = AuthorshipRoles.fromString(role.getAs("name"));
								if(r != null){
									authorRole.setRole(r);
									authorRole.setValue(role.getAs("value"));
									authorRole.setSchema(schema);
								}
							}
							authorRole.setText(role.getAs("name"));

							roles.add(authorRole);
						}
					}

					// --- Costruzione Authorship ---
					Authorship authorship = new Authorship();
					authorship.setProduct(id);
					authorship.setPerson(orcid);
					authorship.setCorresponding(corresponding);
					authorship.setDeclaredAffiliations(declaredAffiliations);
					authorship.setRoles(roles);
					authorship.setCollectedfrom(OPENAIRE_COLLECTED_FROM);
					authorship.setDataInfo(OPENAIRE_DATAINFO);
					return authorship;
				},
				Encoders.bean(Authorship.class)
		).write()
				.mode(SaveMode.Overwrite)
				.option("compression", "gzip")
				.json(outputPath);
		;
	}

	// PROJECT
	private static void extractInfoForActionSetFromProjects(
		String dbUrl, String dbUser, String dbPassword, String hdfsPath, String hdfsNameNode, Boolean exec)
		throws IOException {

		if (exec) {
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
				throw new RuntimeException(e);
			}
		}

	}

	public static ProjectParticipation getRelationWithProject(ResultSet rs) {
		try {
			return getProjectRelation(
				rs.getString("project"), rs.getString("pid"),
				rs.getString("role"));
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static ProjectParticipation getProjectRelation(String project, String orcid, String role) {

		String source = Constants.getPersonId(orcid);

		String target = PROJECT_ID_PREFIX + StringUtils.substringBefore(project, SEPARATOR) + SEPARATOR
			+ IdentifierFactory.md5(StringUtils.substringAfter(project, SEPARATOR));
		ProjectParticipation pp = new ProjectParticipation();
		pp.setPerson(source);
		pp.setProject(target);

		if (StringUtils.isNotBlank(role)) {
			pp.setRoleInProject(ProjectRoles.mapStringToEnum(role));
		}

		pp.setCollectedfrom(OPENAIRE_COLLECTED_FROM);
		pp.setDataInfo(FUNDERDATAINFO);
		return pp;

	}

	protected static void writeRelation(final ProjectParticipation relation, BufferedWriter writer) {
		try {
			writer.write(OBJECT_MAPPER.writeValueAsString(relation));
			writer.newLine();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	// ORCID
	private static void extractInfoForActionSetFromORCID(SparkSession spark, String inputPath, String workingDir) {
		 writePerson(spark, inputPath, workingDir);
		 writeAffiliations(spark, inputPath, workingDir);
	}

	private static void writeAffiliations(SparkSession spark, String inputPath, String workingDir) {
		Dataset<Employment> employmentDataset = spark
			.read()
			.parquet(inputPath + "Employments")
			.as(Encoders.bean(Employment.class));

		Dataset<Author> authors = spark
			.read()
			.parquet(inputPath + "Authors")
			.as(Encoders.bean(Author.class));

		Dataset<Employment> employment = employmentDataset
			.joinWith(authors, employmentDataset.col("orcid").equalTo(authors.col("orcid")))
			.map((MapFunction<Tuple2<Employment, Author>, Employment>) Tuple2::_1, Encoders.bean(Employment.class));

		employment
			.filter((FilterFunction<Employment>) e -> Optional.ofNullable(e.getAffiliationId()).isPresent())
			.filter((FilterFunction<Employment>) e -> e.getAffiliationId().getSchema().equalsIgnoreCase("ror"))
			.map(
				(MapFunction<Employment, AuthorAffiliation>) ExtractPerson::getAffiliationRelation,
				Encoders.bean(AuthorAffiliation.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(workingDir + "/affiliation");
	}

	private static void writePerson(SparkSession spark, String inputPath, String workingDir) {
		// Mapping all the orcid profiles even if the profile has no visible works
		Dataset<Author> authors = spark
			.read()
			.parquet(inputPath + "Authors")
			.as(Encoders.bean(Author.class));

		authors
			.map((MapFunction<Author, Person>) ExtractPerson::getPerson, Encoders.bean(Person.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(workingDir + "/people");

	}

	private static @NotNull Person getPerson(Author op) {
		Person person = new Person();
		person.setId(Constants.getPersonId(op.getOrcid()));
		person
			.setBiography(
				Optional
					.ofNullable(op.getBiography())

					.orElse(""));
		KeyValue kv = OafMapperUtils.keyValue(ORCID_KEY, ModelConstants.ORCID_DS);
		kv.setDataInfo(null);
		person.setCollectedfrom(Collections.singletonList(kv));
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
	}


	private static Coauthors extractCoAuthorsRow(Iterator<Row> it) {
		Coauthors coauth = new Coauthors();
		List<String> coauthors = new ArrayList<>();
		while (it.hasNext())
			coauthors.add(it.next().getAs("orcid"));
		coauth.setCoauthors(coauthors);

		return coauth;
	}

	private static AuthorAffiliation getAffiliationRelation(Employment row) {
		AuthorAffiliation aa = new AuthorAffiliation();

		String source = Constants.getPersonId(row.getOrcid());
		String target = ROR_PREFIX
			+ IdentifierFactory.md5(PidCleaner.normalizePidValue("ROR", row.getAffiliationId().getValue()));
		aa.setPerson(source);
		aa.setOrganization(target);

		Period p =new Period();
		if (Optional.ofNullable(row.getStartDate()).isPresent() && StringUtils.isNotBlank(row.getStartDate())) {

			p.setStartDate(row.getStartDate());

		}
		if (Optional.ofNullable(row.getEndDate()).isPresent() && StringUtils.isNotBlank(row.getEndDate())) {
			p.setEndDate(row.getEndDate());

		}
		aa.setPeriod(Collections.singletonList(p));

		aa.setCollectedfrom(Arrays.asList(OafMapperUtils.keyValue(ORCID_KEY, ModelConstants.ORCID_DS)));
		aa.setDataInfo(ORCIDDATAINFO);

		return aa;

	}


	// ACTION SET
	private static void createActionSet(SparkSession spark, String outputPath, String workingDir) {

		Dataset<Person> people;
		people = spark
			.read()
			.textFile(workingDir + "/people")
			.map(
				(MapFunction<String, Person>) value -> OBJECT_MAPPER
					.readValue(value, Person.class),
				Encoders.bean(Person.class));

//		Dataset<Authorship> authorshipDataset = spark.read().textFile(workingDir + "/authorship")
//				.map((MapFunction<String, Authorship>) values -> OBJECT_MAPPER.readValue(values, Authorship.class),
//						Encoders.bean(Authorship.class));

		Dataset<AuthorAffiliation> authorAffiliationDataset = spark.read().textFile(workingDir + "/affiliation")
				.map((MapFunction<String, AuthorAffiliation>) values -> OBJECT_MAPPER.readValue(values, AuthorAffiliation.class),
						Encoders.bean(AuthorAffiliation.class));

		Dataset<ProjectParticipation> projectParticipationDataset = spark.read().textFile(workingDir + "/project")
				.map((MapFunction<String, ProjectParticipation>) values -> OBJECT_MAPPER.readValue(values, ProjectParticipation.class),
						Encoders.bean(ProjectParticipation.class));

		people
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
//			.union(
//					authorshipDataset
//							.toJavaRDD()
//									.map(r -> new AtomicAction(r.getClass(), r))
//			)
				.union(authorAffiliationDataset
						.toJavaRDD()
						.map(r -> new AtomicAction(r.getClass(), r))
				)
				.union(projectParticipationDataset
						.toJavaRDD()
						.map(r -> new AtomicAction(r.getClass(), r))
				)
				.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(
				outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, BZip2Codec.class);
	}
}
