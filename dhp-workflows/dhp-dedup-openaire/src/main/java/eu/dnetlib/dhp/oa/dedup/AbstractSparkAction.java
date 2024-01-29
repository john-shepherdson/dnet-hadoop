
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;

abstract class AbstractSparkAction implements Serializable {

	protected static final int NUM_PARTITIONS = 1000;
	protected static final int NUM_CONNECTIONS = 20;

	protected static final String TYPE_VALUE_SEPARATOR = "###";
	protected static final String SP_SEPARATOR = "@@@";

	protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	public final ArgumentApplicationParser parser; // parameters for the spark action
	public final SparkSession spark; // the spark session

	protected AbstractSparkAction(ArgumentApplicationParser parser, SparkSession spark) {

		this.parser = parser;
		this.spark = spark;
	}

	public List<DedupConfig> getConfigurations(ISLookUpService isLookUpService, String orchestrator)
		throws ISLookUpException, DocumentException, IOException, SAXException {

		final String xquery = String.format("/RESOURCE_PROFILE[.//DEDUPLICATION/ACTION_SET/@id = '%s']", orchestrator);

		String orchestratorProfile = isLookUpService.getResourceProfileByQuery(xquery);

		final SAXReader reader = new SAXReader();
		reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

		final Document doc = reader.read(new StringReader(orchestratorProfile));

		final String actionSetId = doc.valueOf("//DEDUPLICATION/ACTION_SET/@id");

		final List<DedupConfig> configurations = new ArrayList<>();

		for (final Object o : doc.selectNodes("//SCAN_SEQUENCE/SCAN")) {
			configurations.add(loadConfig(isLookUpService, actionSetId, o));
		}

		return configurations;
	}

	private DedupConfig loadConfig(
		final ISLookUpService isLookUpService, final String actionSetId, final Object o)
		throws ISLookUpException, IOException {
		final Element s = (Element) o;
		final String configProfileId = s.attributeValue("id");
		final String conf = isLookUpService
			.getResourceProfileByQuery(
				String
					.format(
						"for $x in /RESOURCE_PROFILE[.//RESOURCE_IDENTIFIER/@value = '%s'] return $x//DEDUPLICATION/text()",
						configProfileId));

		DedupConfig dedupConfig = DedupConfig.load(conf);
		dedupConfig.getWf().setConfigurationId(actionSetId);

		return dedupConfig;
	}

	abstract void run(ISLookUpService isLookUpService)
		throws DocumentException, IOException, ISLookUpException, SAXException;

	protected static SparkSession getSparkSession(SparkConf conf) {
		return SparkSession.builder().config(conf).getOrCreate();
	}

	protected static SparkSession getSparkWithHiveSession(SparkConf conf) {
		return SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();
	}

	protected static <T> void save(Dataset<T> dataset, String outPath, SaveMode mode) {
		dataset.write().option("compression", "gzip").mode(mode).json(outPath);
	}

	protected static <T> void saveParquet(Dataset<T> dataset, String outPath, SaveMode mode) {
		dataset.write().option("compression", "gzip").mode(mode).parquet(outPath);
	}

	protected static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	protected static String structuredPropertyListToString(List<StructuredProperty> list) {

		return list
			.stream()
			.filter(p -> p.getQualifier() != null)
			.filter(p -> StringUtils.isNotBlank(p.getQualifier().getClassid()))
			.filter(p -> StringUtils.isNotBlank(p.getValue()))
			.map(p -> p.getValue() + TYPE_VALUE_SEPARATOR + p.getQualifier().getClassid())
			.collect(Collectors.joining(SP_SEPARATOR));
	}

	protected static MapFunction<String, Relation> patchRelFn() {
		return value -> {
			final Relation rel = OBJECT_MAPPER.readValue(value, Relation.class);
			if (rel.getDataInfo() == null) {
				rel.setDataInfo(new DataInfo());
			}
			return rel;
		};
	}

	protected boolean isOpenorgs(Relation rel) {
		return Optional
			.ofNullable(rel.getCollectedfrom())
			.map(c -> isCollectedFromOpenOrgs(c))
			.orElse(false);
	}

	protected boolean isOpenorgsDedupRel(Relation rel) {
		return isOpenorgs(rel) && isOpenOrgsDedupMergeRelation(rel);
	}

	private boolean isCollectedFromOpenOrgs(List<KeyValue> c) {
		return c
			.stream()
			.filter(Objects::nonNull)
			.anyMatch(kv -> ModelConstants.OPENORGS_NAME.equals(kv.getValue()));
	}

	private boolean isOpenOrgsDedupMergeRelation(Relation rel) {
		return ModelConstants.ORG_ORG_RELTYPE.equals(rel.getRelType()) &&
			ModelConstants.DEDUP.equals(rel.getSubRelType())
			&& (ModelConstants.IS_MERGED_IN.equals(rel.getRelClass()) ||
				ModelConstants.MERGES.equals(rel.getRelClass()));
	}

	protected static Boolean parseECField(Field<String> field) {
		if (field == null)
			return null;
		if (StringUtils.isBlank(field.getValue()) || field.getValue().equalsIgnoreCase("null"))
			return null;
		return field.getValue().equalsIgnoreCase("true");
	}
}
