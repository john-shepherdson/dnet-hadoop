
package eu.dnetlib.dhp.oa.graph.clean;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.BufferedInputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils;
import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class CleanGraphSparkJob {

	private static final Logger log = LoggerFactory.getLogger(CleanGraphSparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				CleanGraphSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

		final ISLookUpService isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl);
		final VocabularyGroup vocs = VocabularyGroup.loadVocsFromIS(isLookupService);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				fixGraphTable(spark, vocs, inputPath, entityClazz, outputPath);
			});
	}

	private static <T extends Oaf> void fixGraphTable(
		SparkSession spark,
		VocabularyGroup vocs,
		String inputPath,
		Class<T> clazz,
		String outputPath) {

		final CleaningRuleMap mapping = CleaningRuleMap.create(vocs);

		readTableFromPath(spark, inputPath, clazz)
			.map((MapFunction<T, T>) value -> OafCleaner.apply(value, mapping), Encoders.bean(clazz))
			.map((MapFunction<T, T>) value -> fixDefaults(value), Encoders.bean(clazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static <T extends Oaf> T fixDefaults(T value) {
		if (value instanceof Datasource) {
			// nothing to clean here
		} else if (value instanceof Project) {
			// nothing to clean here
		} else if (value instanceof Organization) {
			Organization o = (Organization) value;
			if (Objects.isNull(o.getCountry()) || StringUtils.isBlank(o.getCountry().getClassid())) {
				o.setCountry(qualifier("UNKNOWN", "Unknown", ModelConstants.DNET_COUNTRY_TYPE));
			}
		} else if (value instanceof Relation) {
			// nothing to clean here
		} else if (value instanceof Result) {

			Result r = (Result) value;
			if (Objects.isNull(r.getLanguage()) || StringUtils.isBlank(r.getLanguage().getClassid())) {
				r
					.setLanguage(
						qualifier("und", "Undetermined", ModelConstants.DNET_LANGUAGES));
			}
			if (Objects.nonNull(r.getSubject())) {
				r
					.setSubject(
						r
							.getSubject()
							.stream()
							.filter(Objects::nonNull)
							.filter(sp -> StringUtils.isNotBlank(sp.getValue()))
							.filter(sp -> Objects.nonNull(sp.getQualifier()))
							.filter(sp -> StringUtils.isNotBlank(sp.getQualifier().getClassid()))
							.collect(Collectors.toList()));
			}
			if (Objects.isNull(r.getResourcetype()) || StringUtils.isBlank(r.getResourcetype().getClassid())) {
				r
					.setResourcetype(
						qualifier("UNKNOWN", "Unknown", ModelConstants.DNET_DATA_CITE_RESOURCE));
			}
			if (Objects.isNull(r.getBestaccessright()) || StringUtils.isBlank(r.getBestaccessright().getClassid())) {
				r
					.setBestaccessright(
						qualifier("UNKNOWN", "not available", ModelConstants.DNET_ACCESS_MODES));
			}
			if (Objects.nonNull(r.getInstance())) {
				for (Instance i : r.getInstance()) {
					if (Objects.isNull(i.getAccessright()) || StringUtils.isBlank(i.getAccessright().getClassid())) {
						i.setAccessright(qualifier("UNKNOWN", "not available", ModelConstants.DNET_ACCESS_MODES));
					}
					if (Objects.isNull(i.getHostedby()) || StringUtils.isBlank(i.getHostedby().getKey())) {
						i.setHostedby(ModelConstants.UNKNOWN_REPOSITORY);
					}
					if (Objects.isNull(i.getRefereed())) {
						i.setRefereed(qualifier("0000", "Unknown", ModelConstants.DNET_REVIEW_LEVELS));
					}
				}
			}

			if (value instanceof Publication) {

			} else if (value instanceof eu.dnetlib.dhp.schema.oaf.Dataset) {

			} else if (value instanceof OtherResearchProduct) {

			} else if (value instanceof Software) {

			}
		}

		return value;
	}

	private static Qualifier qualifier(String classid, String classname, String scheme) {
		return OafMapperUtils
			.qualifier(
				classid, classname, scheme, scheme);
	}

	private static <T extends Oaf> Dataset<T> readTableFromPath(
		SparkSession spark, String inputEntityPath, Class<T> clazz) {

		log.info("Reading Graph table from: {}", inputEntityPath);
		return spark
			.read()
			.textFile(inputEntityPath)
			.map(
				(MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, clazz),
				Encoders.bean(clazz));
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

}
