
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

/**
 * @author miriam.baglioni
 * @Date 21/07/22
 */
public class SparkEoscBulkTag implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkEoscBulkTag.class);
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();
		CommunityConfiguration cc;

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, workingPath);
				execBulkTag(spark, inputPath, workingPath, datasourceMapPath, resultClazz);
			});
	}

	private static <R extends Result> void execBulkTag(
		SparkSession spark,
		String inputPath,
		String workingPath,
		String datasourceMapPath,
		Class<R> resultClazz) {

		List<String> hostedByList = readPath(spark, datasourceMapPath, DatasourceMaster.class)
			.map((MapFunction<DatasourceMaster, String>) dm -> dm.getMaster(), Encoders.STRING())
			.collectAsList();

		readPath(spark, inputPath, resultClazz)
			.map(patchResult(), Encoders.bean(resultClazz))
			.filter(Objects::nonNull)
			.map(
				(MapFunction<R, R>) value -> enrich(value, hostedByList),
				Encoders.bean(resultClazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath);

		readPath(spark, workingPath, resultClazz)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(inputPath);

	}

	private static <R extends Result> R enrich(R value, List<String> hostedByList) {
		if (value.getInstance().stream().anyMatch(i -> hostedByList.contains(i.getHostedby().getKey())) ||
			(value.getEoscifguidelines() != null && value.getEoscifguidelines().size() > 0)) {
			Context context = new Context();
			context.setId("eosc");
			context.setDataInfo(Arrays.asList(OafMapperUtils
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
