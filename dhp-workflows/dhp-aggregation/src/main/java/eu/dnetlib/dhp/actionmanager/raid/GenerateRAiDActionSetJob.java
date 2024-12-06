
package eu.dnetlib.dhp.actionmanager.raid;

import static eu.dnetlib.dhp.actionmanager.personentity.ExtractPerson.OPENAIRE_DATASOURCE_ID;
import static eu.dnetlib.dhp.actionmanager.personentity.ExtractPerson.OPENAIRE_DATASOURCE_NAME;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.*;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.raid.model.RAiDEntity;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.Constants;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

public class GenerateRAiDActionSetJob {

	private static final Logger log = LoggerFactory
		.getLogger(eu.dnetlib.dhp.actionmanager.raid.GenerateRAiDActionSetJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final List<KeyValue> RAID_COLLECTED_FROM = listKeyValues(
		OPENAIRE_DATASOURCE_ID, OPENAIRE_DATASOURCE_NAME);

	private static final Qualifier RAID_QUALIFIER = qualifier(
		"0049", "Research Activity Identifier", DNET_PUBLICATION_RESOURCE, DNET_PUBLICATION_RESOURCE);

	private static final Qualifier RAID_INFERENCE_QUALIFIER = qualifier(
		"raid:openaireinference", "Inferred by OpenAIRE", DNET_PROVENANCE_ACTIONS, DNET_PROVENANCE_ACTIONS);

	private static final DataInfo RAID_DATA_INFO = dataInfo(
		false, OPENAIRE_DATASOURCE_NAME, true, false, RAID_INFERENCE_QUALIFIER, "0.92");

	public static void main(final String[] args) throws Exception {

		final String jsonConfiguration = IOUtils
			.toString(
				eu.dnetlib.dhp.actionmanager.raid.GenerateRAiDActionSetJob.class
					.getResourceAsStream("/eu/dnetlib/dhp/actionmanager/raid/action_set_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			removeOutputDir(spark, outputPath);
			processRAiDEntities(spark, inputPath, outputPath);
		});
	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	static void processRAiDEntities(final SparkSession spark,
		final String inputPath,
		final String outputPath) {
		readInputPath(spark, inputPath)
			.map(GenerateRAiDActionSetJob::prepareRAiD)
			.flatMap(List::iterator)
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

	}

	protected static List<AtomicAction<? extends Oaf>> prepareRAiD(final RAiDEntity r) {

		final Date now = new Date();
		final OtherResearchProduct orp = new OtherResearchProduct();
		final List<AtomicAction<? extends Oaf>> res = new ArrayList<>();
		String raidId = calculateOpenaireId(r.getRaid());

		orp.setId(raidId);
		orp.setCollectedfrom(RAID_COLLECTED_FROM);
		orp.setDataInfo(RAID_DATA_INFO);
		orp
			.setTitle(
				Collections
					.singletonList(
						structuredProperty(
							r.getTitle(),
							qualifier("main title", "main title", DNET_DATACITE_TITLE, DNET_DATACITE_TITLE),
							RAID_DATA_INFO)));
		orp.setDescription(listFields(RAID_DATA_INFO, r.getSummary()));
//		orp.setAuthor(createAuthors(r.getAuthors()));
		orp.setInstance(Collections.singletonList(eu.dnetlib.dhp.actionmanager.Constants.getInstance(RAID_QUALIFIER)));
		orp
			.setSubject(
				r
					.getSubjects()
					.stream()
					.map(
						s -> subject(
							s,
							qualifier(
								DNET_SUBJECT_KEYWORD, DNET_SUBJECT_KEYWORD, DNET_SUBJECT_TYPOLOGIES,
								DNET_SUBJECT_TYPOLOGIES),
							RAID_DATA_INFO))
					.collect(Collectors.toList()));
		orp
			.setRelevantdate(
				Arrays
					.asList(
						structuredProperty(
							r.getEndDate(), qualifier("endDate", "endDate", DNET_DATACITE_DATE, DNET_DATACITE_DATE),
							RAID_DATA_INFO),
						structuredProperty(
							r.getStartDate(),
							qualifier("startDate", "startDate", DNET_DATACITE_DATE, DNET_DATACITE_DATE),
							RAID_DATA_INFO)));
		orp.setLastupdatetimestamp(now.getTime());
		orp.setDateofacceptance(field(r.getStartDate(), RAID_DATA_INFO));

		res.add(new AtomicAction<>(OtherResearchProduct.class, orp));

		for (String resultId : r.getIds()) {
			Relation rel1 = OafMapperUtils
				.getRelation(
					raidId,
					resultId,
					ModelConstants.RESULT_RESULT,
					PART,
					HAS_PART,
					RAID_COLLECTED_FROM,
					RAID_DATA_INFO,
					now.getTime(),
					null,
					null);
			Relation rel2 = OafMapperUtils
				.getRelation(
					resultId,
					raidId,
					ModelConstants.RESULT_RESULT,
					PART,
					IS_PART_OF,
					RAID_COLLECTED_FROM,
					RAID_DATA_INFO,
					now.getTime(),
					null,
					null);
			res.add(new AtomicAction<>(Relation.class, rel1));
			res.add(new AtomicAction<>(Relation.class, rel2));
		}

		return res;
	}

	public static String calculateOpenaireId(final String raid) {
		return String.format("50|%s::%s", Constants.RAID_NS_PREFIX, DHPUtils.md5(raid));
	}

	public static List<Author> createAuthors(final List<String> author) {
		return author.stream().map(s -> {
			Author a = new Author();
			a.setFullname(s);
			return a;
		}).collect(Collectors.toList());
	}

	private static JavaRDD<RAiDEntity> readInputPath(
		final SparkSession spark,
		final String path) {

		return spark
			.read()
			.json(path)
			.as(Encoders.bean(RAiDEntity.class))
			.toJavaRDD();

	}

}
