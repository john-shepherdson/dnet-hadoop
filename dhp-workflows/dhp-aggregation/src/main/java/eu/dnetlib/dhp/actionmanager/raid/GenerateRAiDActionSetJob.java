
package eu.dnetlib.dhp.actionmanager.raid;

import static eu.dnetlib.dhp.actionmanager.personentity.ASConstants.OPENAIRE_DATASOURCE_ID;
import static eu.dnetlib.dhp.actionmanager.personentity.ASConstants.OPENAIRE_DATASOURCE_NAME;
import static eu.dnetlib.dhp.common.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.*;

import java.util.*;

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
		"0049", "Research Activity", DNET_PUBLICATION_RESOURCE, DNET_PUBLICATION_RESOURCE);

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
		log.info("outputPath: {} ", outputPath);

		final String baseUrl = parser.get("baseUrl");
		log.info("baseUrl: {}", baseUrl);

		final String graphBasePath = parser.get("graphBasePath");
		log.info("graphBasePath: {}", graphBasePath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			removeOutputDir(spark, outputPath);
			saveActionSet(spark, inputPath, outputPath, baseUrl, graphBasePath);
		});
	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	static void saveActionSet(final SparkSession spark, final String inputPath, final String outputPath,
		final String baseUrl, final String graphBasePath) {
		// save result in the action set
		raidEntitiesToAtomicActions(spark, inputPath, baseUrl, graphBasePath)
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);
	}

	static JavaRDD<AtomicAction<? extends Oaf>> raidEntitiesToAtomicActions(final SparkSession spark,
		final String inputPath, final String baseUrl, final String graphBasePath) {

		// prepare RAiD entities as ORP and their relations with result entities
		JavaRDD<? extends Oaf> graphEntities = readInputPath(spark, inputPath)
			.map(r -> rawRAiDtoGraphEntities(r, baseUrl))
			.flatMap(List::iterator);

		// collect relations between RAiD entity and result entities
		JavaRDD<Relation> raidToResultRels = graphEntities
			.filter(e -> e instanceof Relation)
			.map(e -> (Relation) e)
			.filter(rel -> rel.getRelClass().equals(HAS_PART));

		// collect relations between result entities and projects/organizations/merged
		JavaRDD<Relation> relevantRels = readRelevantRelations(spark, graphBasePath);

		// redirect relations to create new relations to be added to the action set
		JavaRDD<Relation> newRels = relevantRels
			.mapToPair(rel -> new Tuple2<>(rel.getSource(), rel))
			.join(raidToResultRels.mapToPair(rel -> new Tuple2<>(rel.getTarget(), rel)))
			.flatMap(x -> {
				Relation leftRel = x._2()._1();
				String entityId = leftRel.getTarget();
				String relType = leftRel.getRelType();
				Relation raidRel = x._2()._2();
				String raidId = x._2()._2().getSource();

				List<Relation> res = new ArrayList<>();

				Relation rel1 = getRelation(
					raidId,
					entityId,
					relType,
					PART,
					HAS_PART,
					raidRel.getCollectedfrom(),
					raidRel.getDataInfo(),
					raidRel.getLastupdatetimestamp());

				Relation rel2 = getRelation(
					entityId,
					raidId,
					relType,
					PART,
					IS_PART_OF,
					raidRel.getCollectedfrom(),
					raidRel.getDataInfo(),
					raidRel.getLastupdatetimestamp());

				res.add(rel1);
				res.add(rel2);
				return res.iterator();
			})
			.distinct();

		// create actions
		return graphEntities
			.map(
				e -> (e instanceof Relation) ? new AtomicAction<>(Relation.class, (Relation) e)
					: new AtomicAction<>(OtherResearchProduct.class, (OtherResearchProduct) e))
			.union(newRels.map(rel -> new AtomicAction<>(Relation.class, rel))); // actions for new relations

	}

	private static JavaRDD<Relation> readRelevantRelations(final SparkSession spark, final String graphBasePath) {
		// take only relations between results and projects/organizations/merged
		return spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(graphBasePath + "/relation")
			.as(Encoders.bean(Relation.class))
			.toJavaRDD()
			.filter(rel -> !rel.getDataInfo().getDeletedbyinference())
			.filter(
				rel -> rel.getRelType().equals(RESULT_PROJECT) || rel.getRelType().equals(RESULT_ORGANIZATION)
					|| rel.getRelType().equals(RESULT_RESULT))
			.filter(
				rel -> rel.getRelClass().equals(IS_PRODUCED_BY) || rel.getRelClass().equals(HAS_AUTHOR_INSTITUTION)
					|| rel.getRelClass().equals(MERGES));
	}

	protected static List<Oaf> rawRAiDtoGraphEntities(final RAiDEntity r, final String baseUrl) {
		final Date now = new Date();
		final OtherResearchProduct orp = new OtherResearchProduct(); // ORP to contain the RAiD entity in the graph
		final List<Oaf> res = new ArrayList<>();

		// populate the ORP
		String raidId = calculateOpenaireId(r.getId());
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
		orp.setDescription(listFields(RAID_DATA_INFO, r.getDescription()));

		Instance instance = new Instance();
		instance.setInstancetype(RAID_QUALIFIER);
		instance.setUrl(Collections.singletonList(baseUrl + raidId.split("\\|")[1]));
		orp.setInstance(Collections.singletonList(instance));
//		orp
//				.setSubject(
//						r
//								.getSubjects()
//								.stream()
//								.map(
//										s -> subject(
//												s,
//												qualifier(
//														DNET_SUBJECT_KEYWORD, DNET_SUBJECT_KEYWORD, DNET_SUBJECT_TYPOLOGIES,
//														DNET_SUBJECT_TYPOLOGIES),
//												RAID_DATA_INFO))
//								.collect(Collectors.toList()));
		orp
			.setRelevantdate(
				Arrays
					.asList(
						structuredProperty(
							r.getEndDate(), qualifier(END_DATE, END_DATE, DNET_DATACITE_DATE, DNET_DATACITE_DATE),
							RAID_DATA_INFO),
						structuredProperty(
							r.getStartDate(),
							qualifier(START_DATE, START_DATE, DNET_DATACITE_DATE, DNET_DATACITE_DATE),
							RAID_DATA_INFO)));
		orp.setLastupdatetimestamp(now.getTime());
		orp.setDateofacceptance(field(r.getStartDate(), RAID_DATA_INFO));

		res.add(orp);

		// create relations between the ORP RAiD entity and its research products
		for (String resultId : r.getIds()) {
			Relation rel1 = OafMapperUtils
				.getRelation(
					raidId,
					resultId,
					ModelConstants.RESULT_RESULT,
					PART,
					HAS_PART,
					orp);
			Relation rel2 = OafMapperUtils
				.getRelation(
					resultId,
					raidId,
					ModelConstants.RESULT_RESULT,
					PART,
					IS_PART_OF,
					orp);
			res.add(rel1);
			res.add(rel2);
		}

		return res;
	}

	public static String calculateOpenaireId(final String raid) {
		return String.format("50|%s::%s", RAID_NS_PREFIX, DHPUtils.md5(raid));
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
