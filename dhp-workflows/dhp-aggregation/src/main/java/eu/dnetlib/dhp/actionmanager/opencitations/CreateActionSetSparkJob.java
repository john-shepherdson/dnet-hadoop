
package eu.dnetlib.dhp.actionmanager.opencitations;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.opencitations.model.COCI;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import scala.Tuple2;

public class CreateActionSetSparkJob implements Serializable {
	public static final String OPENCITATIONS_CLASSID = "sysimport:crosswalk:opencitations";
	public static final String OPENCITATIONS_CLASSNAME = "Imported from OpenCitations";
	private static final String DOI_PREFIX = "50|doi_________::";

	private static final String PMID_PREFIX = "50|pmid________::";
	private static final String TRUST = "0.91";

	private static final Logger log = LoggerFactory.getLogger(CreateActionSetSparkJob.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws IOException, ParseException {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							CreateActionSetSparkJob.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/actionmanager/opencitations/as_parameters.json"))));

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}", inputPath.toString());

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}", outputPath);

		final String prefix = parser.get("prefix");
		log.info("prefix {}", prefix);

		final boolean shouldDuplicateRels = Optional
			.ofNullable(parser.get("shouldDuplicateRels"))
			.map(Boolean::valueOf)
			.orElse(Boolean.FALSE);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				extractContent(spark, inputPath, outputPath, shouldDuplicateRels, prefix);
			});

	}

	private static void extractContent(SparkSession spark, String inputPath, String outputPath,
		boolean shouldDuplicateRels, String prefix) {
		spark
			.read()
			.textFile(inputPath + "/*")
			.map(
				(MapFunction<String, COCI>) value -> OBJECT_MAPPER.readValue(value, COCI.class),
				Encoders.bean(COCI.class))
			.flatMap(
				(FlatMapFunction<COCI, Relation>) value -> createRelation(value, shouldDuplicateRels, prefix)
					.iterator(),
				Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) value -> value != null)
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

	}

	private static List<Relation> createRelation(COCI value, boolean duplicate, String p) {

		List<Relation> relationList = new ArrayList<>();
		String prefix;
		if (p.equals("COCI")) {
			prefix = DOI_PREFIX;
		} else {
			prefix = PMID_PREFIX;
		}

		String citing = prefix
			+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", value.getCiting()));
		final String cited = prefix
			+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", value.getCited()));

		if (!citing.equals(cited)) {
			relationList
				.add(
					getRelation(
						citing,
						cited, ModelConstants.CITES));

			if (duplicate && value.getCiting().endsWith(".refs")) {
				citing = prefix + IdentifierFactory
					.md5(
						CleaningFunctions
							.normalizePidValue(
								"doi", value.getCiting().substring(0, value.getCiting().indexOf(".refs"))));
				relationList.add(getRelation(citing, cited, ModelConstants.CITES));
			}
		}

		return relationList;
	}

	private static Collection<Relation> getRelations(String citing, String cited) {

		return Arrays
			.asList(
				getRelation(citing, cited, ModelConstants.CITES),
				getRelation(cited, citing, ModelConstants.IS_CITED_BY));
	}

	public static Relation getRelation(
		String source,
		String target,
		String relclass) {
		Relation r = new Relation();
		r.setCollectedfrom(getCollectedFrom());
		r.setSource(source);
		r.setTarget(target);
		r.setRelClass(relclass);
		r.setRelType(ModelConstants.RESULT_RESULT);
		r.setSubRelType(ModelConstants.CITATION);
		r
			.setDataInfo(
				getDataInfo());
		return r;
	}

	public static List<KeyValue> getCollectedFrom() {
		KeyValue kv = new KeyValue();
		kv.setKey(ModelConstants.OPENOCITATIONS_ID);
		kv.setValue(ModelConstants.OPENOCITATIONS_NAME);

		return Arrays.asList(kv);
	}

	public static DataInfo getDataInfo() {
		DataInfo di = new DataInfo();
		di.setInferred(false);
		di.setDeletedbyinference(false);
		di.setTrust(TRUST);

		di
			.setProvenanceaction(
				getQualifier(OPENCITATIONS_CLASSID, OPENCITATIONS_CLASSNAME, ModelConstants.DNET_PROVENANCE_ACTIONS));
		return di;
	}

	public static Qualifier getQualifier(String class_id, String class_name,
		String qualifierSchema) {
		Qualifier pa = new Qualifier();
		pa.setClassid(class_id);
		pa.setClassname(class_name);
		pa.setSchemeid(qualifierSchema);
		pa.setSchemename(qualifierSchema);
		return pa;
	}

}
