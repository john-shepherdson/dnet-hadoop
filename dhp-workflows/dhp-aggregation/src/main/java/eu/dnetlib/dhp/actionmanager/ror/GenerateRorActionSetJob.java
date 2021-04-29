
package eu.dnetlib.dhp.actionmanager.ror;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.schema.common.ModelConstants.ENTITYREGISTRY_PROVENANCE_ACTION;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.dataInfo;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.field;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.listKeyValues;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.qualifier;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.structuredProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.SparkAtomicActionJob;
import eu.dnetlib.dhp.actionmanager.ror.model.RorOrganization;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

public class GenerateRorActionSetJob {

	private static final String COUNTRIES_VOC = "dnet:countries";

	private static final Logger log = LoggerFactory.getLogger(GenerateRorActionSetJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final List<KeyValue> ROR_COLLECTED_FROM = listKeyValues("10|openaire____::993a7ae7a863813cf95028b50708e222", "ROR");

	private static final DataInfo ROR_DATA_INFO = dataInfo(false, "", false, false, ENTITYREGISTRY_PROVENANCE_ACTION, "0.92");

	private static final Qualifier ROR_PID_TYPE = qualifier("ROR", "ROR", "dnet:pid_types", "dnet:pid_types");
	private static final Qualifier GRID_PID_TYPE = qualifier("GRID", "GRID", "dnet:pid_types", "dnet:pid_types");
	private static final Qualifier WIKIDATA_PID_TYPE = qualifier("Wikidata", "Wikidata", "dnet:pid_types", "dnet:pid_types");
	private static final Qualifier ORGREF_PID_TYPE = qualifier("OrgRef", "OrgRef", "dnet:pid_types", "dnet:pid_types");
	private static final Qualifier ISNI_PID_TYPE = qualifier("ISNI", "ISNI", "dnet:pid_types", "dnet:pid_types");
	private static final Qualifier FUNDREF_PID_TYPE = qualifier("FundRef", "FundRef", "dnet:pid_types", "dnet:pid_types");

	public static void main(final String[] args) throws Exception {

		final String jsonConfiguration = IOUtils
			.toString(SparkAtomicActionJob.class
				.getResourceAsStream("/eu/dnetlib/dhp/actionmanager/ror/action_set_parameters.json"));

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
			processRorOrganizations(spark, inputPath, outputPath);
		});
	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static void processRorOrganizations(final SparkSession spark,
		final String inputPath,
		final String outputPath) {

		readInputPath(spark, inputPath)
			.map(GenerateRorActionSetJob::convertRorOrg, Encoders.bean(Organization.class))
			.toJavaRDD()
			.map(o -> new AtomicAction<>(Organization.class, o))
			.mapToPair(aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
				new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

	}

	protected static Organization convertRorOrg(final RorOrganization r) {

		final Date now = new Date();

		final Organization o = new Organization();

		o.setId(String.format("20|ror_________::%s", DHPUtils.md5(r.getId())));
		o.setOriginalId(Arrays.asList(r.getId()));
		o.setCollectedfrom(ROR_COLLECTED_FROM);
		o.setPid(pids(r));
		o.setDateofcollection(now.toString());
		o.setDateoftransformation(now.toString());
		o.setExtraInfo(new ArrayList<>()); // Values not present in the file
		o.setOaiprovenance(null); // Values not present in the file
		o.setLegalshortname(field(r.getAcronyms().stream().findFirst().orElse(r.getName()), ROR_DATA_INFO));
		o.setLegalname(field(r.getName(), ROR_DATA_INFO));
		o.setAlternativeNames(alternativeNames(r));
		o.setWebsiteurl(field(r.getLinks().stream().findFirst().orElse(null), ROR_DATA_INFO));
		o.setLogourl(null);
		o.setEclegalbody(null);
		o.setEclegalperson(null);
		o.setEcnonprofit(null);
		o.setEcresearchorganization(null);
		o.setEchighereducation(null);
		o.setEcinternationalorganizationeurinterests(null);
		o.setEcinternationalorganization(null);
		o.setEcenterprise(null);
		o.setEcsmevalidated(null);
		o.setEcnutscode(null);
		if (r.getCountry() != null) {
			o.setCountry(qualifier(r.getCountry().getCountryCode(), r.getCountry().getCountryName(), COUNTRIES_VOC, COUNTRIES_VOC));
		} else {
			o.setCountry(null);
		}
		o.setDataInfo(ROR_DATA_INFO);
		o.setLastupdatetimestamp(now.getTime());

		return o;
	}

	private static List<StructuredProperty> pids(final RorOrganization r) {
		final List<StructuredProperty> pids = new ArrayList<>();
		pids.add(structuredProperty(r.getId(), ROR_PID_TYPE, ROR_DATA_INFO));
		pids.add(structuredProperty(r.getExternalIds().getGrid().getAll(), GRID_PID_TYPE, ROR_DATA_INFO));
		pids.addAll(pids(r.getExternalIds().getFundRef().getAll(), FUNDREF_PID_TYPE));
		pids.addAll(pids(r.getExternalIds().getIsni().getAll(), ISNI_PID_TYPE));
		pids.addAll(pids(r.getExternalIds().getOrgRef().getAll(), ORGREF_PID_TYPE));
		pids.addAll(pids(r.getExternalIds().getWikidata().getAll(), WIKIDATA_PID_TYPE));
		return pids;
	}

	private static List<StructuredProperty> pids(final List<String> list, final Qualifier pidType) {
		if (list == null) { return new ArrayList<>(); }
		return list.stream()
			.filter(StringUtils::isNotBlank)
			.distinct()
			.map(s -> structuredProperty(s, pidType, ROR_DATA_INFO))
			.collect(Collectors.toList());
	}

	private static List<Field<String>> alternativeNames(final RorOrganization r) {
		final Set<String> names = new LinkedHashSet<>();
		names.addAll(r.getAliases());
		names.addAll(r.getAcronyms());
		r.getLabels().forEach(l -> names.add(l.getLabel()));

		return names.stream().filter(StringUtils::isNotBlank).map(s -> field(s, ROR_DATA_INFO)).collect(Collectors.toList());
	}

	private static Dataset<RorOrganization> readInputPath(
		final SparkSession spark,
		final String inputPath) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, RorOrganization>) value -> OBJECT_MAPPER.readValue(value, RorOrganization.class), Encoders.bean(RorOrganization.class));
	}

}
