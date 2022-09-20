
package eu.dnetlib.dhp.bulktag.eosc;

import static eu.dnetlib.dhp.PropagationConstant.readPath;
import static eu.dnetlib.dhp.PropagationConstant.removeOutputDir;
import static eu.dnetlib.dhp.bulktag.community.TaggingConstants.*;
import static eu.dnetlib.dhp.bulktag.community.TaggingConstants.TAGGING_TRUST;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.schema.common.ModelConstants.DNET_PROVENANCE_ACTIONS;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import javax.print.attribute.DocAttributeSet;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bulktag.SparkBulkTagJob;
import eu.dnetlib.dhp.bulktag.community.*;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import scala.Tuple2;

/**
 * @author miriam.baglioni
 * @Date 21/07/22
 */
public class SparkEoscBulkTag implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkEoscBulkTag.class);
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static String OPENAIRE_3 = "openaire3.0";
	private static String OPENAIRE_4 = "openaire-pub_4.0";
	private static String OPENAIRE_CRIS = "openaire-cris_1.1";
	private static String OPENAIRE_DATA = "openaire2.0_data";

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkEoscBulkTag.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/bulktag/input_eosc_bulkTag_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		String datasourceMapPath = parser.get("datasourceMapPath");
		log.info("datasourceMapPath: {}", datasourceMapPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final String resultType = parser.get("resultType");
		log.info("resultType: {}", resultType);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();
		CommunityConfiguration cc;

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, workingPath);
				selectCompliantDatasources(spark, inputPath, workingPath, datasourceMapPath);
				execBulkTag(spark, inputPath, workingPath, resultType, resultClazz);
			});
	}

	private static void selectCompliantDatasources(SparkSession spark, String inputPath, String workingPath, String datasourceMapPath) {
		Dataset<Datasource> datasources = readPath(spark, inputPath + "datasource", Datasource.class)
				.filter((FilterFunction<Datasource>) ds -> {
					final String compatibility = ds.getOpenairecompatibility().getClassid();
					return compatibility.equalsIgnoreCase(OPENAIRE_3) ||
							compatibility.equalsIgnoreCase(OPENAIRE_4) ||
							compatibility.equalsIgnoreCase(OPENAIRE_CRIS) ||
							compatibility.equalsIgnoreCase(OPENAIRE_DATA);
				});

		Dataset<DatasourceMaster> datasourceMaster = readPath(spark, datasourceMapPath, DatasourceMaster.class);

		datasources.joinWith(datasourceMaster, datasources.col("id").equalTo(datasourceMaster.col("master")), "left")
				.map((MapFunction<Tuple2<Datasource, DatasourceMaster>, DatasourceMaster>) t2 -> t2._2(), Encoders.bean(DatasourceMaster.class) )
				.filter(Objects::nonNull)
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression", "gzip")
				.json(workingPath + "datasource");
	}

	private static <R extends Result> void execBulkTag(
		SparkSession spark,
		String inputPath,
		String workingPath,
		String resultType,
		Class<R> resultClazz) {

		List<String> hostedByList = readPath(spark, workingPath + "datasource", DatasourceMaster.class)
			.map((MapFunction<DatasourceMaster, String>) dm -> dm.getMaster(), Encoders.STRING())
			.collectAsList();

		readPath(spark, inputPath + resultType, resultClazz)
						.map(
				(MapFunction<R, R>) value -> enrich(value, hostedByList),
				Encoders.bean(resultClazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath + resultType);

		readPath(spark, workingPath + resultType, resultClazz)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(inputPath + resultType);

	}

	private static <R extends Result> R enrich(R value, List<String> hostedByList) {
		if (value.getDataInfo().getDeletedbyinference() == null) {
			value.getDataInfo().setDeletedbyinference(false);
		}
		if (value.getContext() == null) {
			value.setContext(new ArrayList<>());
		}
		if (value
			.getInstance()
			.stream()
			.anyMatch(
				i -> (hostedByList.contains(i.getHostedby().getKey())))
			&&
			!value.getContext().stream().anyMatch(c -> c.getId().equals("eosc"))) {
			Context context = new Context();
			context.setId("eosc");
			context
				.setDataInfo(
					Arrays
						.asList(
							OafMapperUtils
								.dataInfo(
									false, BULKTAG_DATA_INFO_TYPE, true, false,
									OafMapperUtils
										.qualifier(
											CLASS_ID_DATASOURCE, CLASS_NAME_BULKTAG_DATASOURCE,
											DNET_PROVENANCE_ACTIONS, DNET_PROVENANCE_ACTIONS),
									TAGGING_TRUST)));
			value.getContext().add(context);

		}
		return value;

	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

	// TODO remove this hack as soon as the values fixed by this method will be provided as NON null
	private static <R extends Result> MapFunction<R, R> patchResult() {
		return r -> {
			if (r.getDataInfo().getDeletedbyinference() == null) {
				r.getDataInfo().setDeletedbyinference(false);
			}
			if (r.getContext() == null) {
				r.setContext(new ArrayList<>());
			}
			return r;
		};
	}

}
