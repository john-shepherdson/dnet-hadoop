
package eu.dnetlib.dhp.actionmanager.ror;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.schema.common.ModelConstants.ENTITYREGISTRY_PROVENANCE_ACTION;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.dataInfo;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.field;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.listKeyValues;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.qualifier;
import static eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.structuredProperty;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.ror.model.ExternalIdType;
import eu.dnetlib.dhp.actionmanager.ror.model.RorOrganization;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.Constants;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

public class GenerateRorActionSetJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateRorActionSetJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final List<KeyValue> ROR_COLLECTED_FROM = listKeyValues(
		Constants.ROR_OPENAIRE_ID, Constants.ROR_DATASOURCE_NAME);

	private static final DataInfo ROR_DATA_INFO = dataInfo(
		false, "", false, false, ENTITYREGISTRY_PROVENANCE_ACTION, "0.92");

	private static final Qualifier ROR_PID_TYPE = qualifier(
		"ROR", "ROR", ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES);

	public static void main(final String[] args) throws Exception {

		final String jsonConfiguration = IOUtils
			.toString(
				GenerateRorActionSetJob.class
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
		final String outputPath) throws IOException {

		readInputPath(spark, inputPath)
			.map(GenerateRorActionSetJob::convertRorOrg)
			.flatMap(List::iterator)
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);
	}

	protected static List<AtomicAction<? extends Oaf>> convertRorOrg(final RorOrganization r) {

		final Date now = new Date();

		final Organization o = new Organization();

		o.setId(calculateOpenaireId(r.getId()));
		o.setOriginalId(Arrays.asList(String.format("%s::%s", Constants.ROR_NS_PREFIX, r.getId())));
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
			o
				.setCountry(
					qualifier(
						r.getCountry().getCountryCode(), r
							.getCountry()
							.getCountryName(),
						ModelConstants.DNET_COUNTRY_TYPE, ModelConstants.DNET_COUNTRY_TYPE));
		} else {
			o.setCountry(null);
		}
		o.setDataInfo(ROR_DATA_INFO);
		o.setLastupdatetimestamp(now.getTime());

		final List<AtomicAction<? extends Oaf>> res = new ArrayList<>();
		res.add(new AtomicAction<>(Organization.class, o));

		return res;

	}

	public static String calculateOpenaireId(final String rorId) {
		return String.format("20|%s::%s", Constants.ROR_NS_PREFIX, DHPUtils.md5(rorId));
	}

	private static List<StructuredProperty> pids(final RorOrganization r) {
		final List<StructuredProperty> pids = new ArrayList<>();
		pids.add(structuredProperty(r.getId(), ROR_PID_TYPE, ROR_DATA_INFO));

		for (final Map.Entry<String, ExternalIdType> e : r.getExternalIds().entrySet()) {
			final String type = e.getKey();
			final List<String> all = e.getValue().getAll();
			if (all != null) {
				final Qualifier qualifier = qualifier(
					type, type, ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES);
				for (final String pid : all) {
					pids
						.add(structuredProperty(pid, qualifier, ROR_DATA_INFO));
				}
			}
		}

		return pids;
	}

	private static List<Field<String>> alternativeNames(final RorOrganization r) {
		final Set<String> names = new LinkedHashSet<>();
		names.addAll(r.getAliases());
		names.addAll(r.getAcronyms());
		r.getLabels().forEach(l -> names.add(l.getLabel()));

		return names
			.stream()
			.filter(StringUtils::isNotBlank)
			.map(s -> field(s, ROR_DATA_INFO))
			.collect(Collectors.toList());
	}

	private static JavaRDD<RorOrganization> readInputPath(
		final SparkSession spark,
		final String path) throws IOException {

		try (final FileSystem fileSystem = FileSystem.get(new Configuration());
			final InputStream is = fileSystem.open(new Path(path))) {
			final RorOrganization[] arr = OBJECT_MAPPER.readValue(is, RorOrganization[].class);
			return spark.createDataset(Arrays.asList(arr), Encoders.bean(RorOrganization.class)).toJavaRDD();
		}
	}

}
