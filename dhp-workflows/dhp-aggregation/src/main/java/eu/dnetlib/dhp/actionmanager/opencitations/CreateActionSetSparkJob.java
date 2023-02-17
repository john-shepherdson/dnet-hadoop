
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
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;
import scala.Tuple2;

public class CreateActionSetSparkJob implements Serializable {
	public static final String OPENCITATIONS_CLASSID = "sysimport:crosswalk:opencitations";
	public static final String OPENCITATIONS_CLASSNAME = "Imported from OpenCitations";
	private static final String ID_PREFIX = "50|doi_________::";
	private static final Float TRUST = 0.91f;
	private static final KeyValue COLLECTED_FROM;

	public static final DataInfo DATA_INFO;

	static {
		COLLECTED_FROM = new KeyValue();
		COLLECTED_FROM.setKey(ModelConstants.OPENOCITATIONS_ID);
		COLLECTED_FROM.setValue(ModelConstants.OPENOCITATIONS_NAME);

		DATA_INFO = OafMapperUtils
			.dataInfo(
				TRUST,
				null,
				false,
				OafMapperUtils
					.qualifier(
						OPENCITATIONS_CLASSID,
						OPENCITATIONS_CLASSNAME,
						ModelConstants.DNET_PROVENANCE_ACTIONS));
	}

	private static final List<Provenance> PROVENANCE = Arrays
		.asList(
			OafMapperUtils.getProvenance(COLLECTED_FROM, DATA_INFO));

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

		final boolean shouldDuplicateRels = Optional
			.ofNullable(parser.get("shouldDuplicateRels"))
			.map(Boolean::valueOf)
			.orElse(Boolean.FALSE);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				extractContent(spark, inputPath, outputPath, shouldDuplicateRels);
			});

	}

	private static void extractContent(SparkSession spark, String inputPath, String outputPath,
		boolean shouldDuplicateRels) {
		spark
			.read()
			.textFile(inputPath + "/*")
			.map(
				(MapFunction<String, COCI>) value -> OBJECT_MAPPER.readValue(value, COCI.class),
				Encoders.bean(COCI.class))
			.flatMap(
				(FlatMapFunction<COCI, Relation>) value -> createRelation(value, shouldDuplicateRels).iterator(),
				Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) value -> value != null)
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

	}

	private static List<Relation> createRelation(COCI value, boolean duplicate) {

		List<Relation> relationList = new ArrayList<>();

		String citing = asOpenAireId(value.getCiting());
		final String cited = asOpenAireId(value.getCited());
		if (!citing.equals(cited)) {
			relationList.add(getRelation(citing, cited));

			if (duplicate && value.getCiting().endsWith(".refs")) {
				citing = asOpenAireId(value.getCiting());
				relationList.add(getRelation(citing, cited));
			}
		}

		return relationList;
	}

	private static String asOpenAireId(String value) {
		return IdentifierFactory
			.idFromPid(
				"50", PidType.doi.toString(),
				CleaningFunctions.normalizePidValue(PidType.doi.toString(), value),
				true);
	}

	public static Relation getRelation(
		String source,
		String target) {
		Relation r = new Relation();
		r.setProvenance(PROVENANCE);
		r.setSource(source);
		r.setTarget(target);
		r.setRelType(ModelConstants.RESULT_RESULT);
		r.setSubRelType(ModelConstants.CITATION);
		r.setRelClass(ModelConstants.CITES);
		return r;
	}

}
