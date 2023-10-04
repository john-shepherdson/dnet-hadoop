
package eu.dnetlib.dhp.swh;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.swh.models.LastVisitData;
import eu.dnetlib.dhp.swh.utils.SWHConstants;
import scala.Tuple2;

/**
 * Creates action sets for Software Heritage data
 *
 * @author Serafeim Chatzopoulos
 */
public class PrepareSWHActionsets {

	private static final Logger log = LoggerFactory.getLogger(PrepareSWHActionsets.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareSWHActionsets.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/swh/input_prepare_swh_actionsets.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("lastVisitsPath");
		log.info("inputPath: {}", inputPath);

		final String softwareInputPath = parser.get("softwareInputPath");
		log.info("softwareInputPath: {}", softwareInputPath);

		final String outputPath = parser.get("actionsetsPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaPairRDD<Text, Text> softwareRDD = prepareActionsets(spark, inputPath, softwareInputPath);
				softwareRDD
					.saveAsHadoopFile(
						outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);
			});
	}

	private static Dataset<Row> loadSWHData(SparkSession spark, String inputPath) {

		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		// read from file and transform to <origin, snapshotId> tuples
		// Note: snapshot id is the SWH id for us
		JavaRDD<Row> swhRDD = sc
			.sequenceFile(inputPath, Text.class, Text.class)
			.map(t -> t._2().toString())
			.map(t -> OBJECT_MAPPER.readValue(t, LastVisitData.class))
			.filter(t -> t.getOrigin() != null && t.getSnapshot() != null) // response from SWH API is empty if repo URL
																			// was not found
			.map(item -> RowFactory.create(item.getOrigin(), item.getSnapshot()));

		// convert RDD to 2-column DF
		List<StructField> fields = Arrays
			.asList(
				DataTypes.createStructField("repoUrl", DataTypes.StringType, true),
				DataTypes.createStructField("swhId", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);

		return spark.createDataFrame(swhRDD, schema);
	}

	private static Dataset<Row> loadGraphSoftwareData(SparkSession spark, String softwareInputPath) {
		return spark
			.read()
			.textFile(softwareInputPath)
			.map(
				(MapFunction<String, Software>) t -> OBJECT_MAPPER.readValue(t, Software.class),
				Encoders.bean(Software.class))
			.filter(t -> t.getCodeRepositoryUrl() != null)
			.select(col("id"), col("codeRepositoryUrl.value").as("repoUrl"));
	}

	private static <I extends Software> JavaPairRDD<Text, Text> prepareActionsets(SparkSession spark, String inputPath,
		String softwareInputPath) {

		Dataset<Row> swhDF = loadSWHData(spark, inputPath);
//		swhDF.show(false);

		Dataset<Row> graphSoftwareDF = loadGraphSoftwareData(spark, softwareInputPath);
//		graphSoftwareDF.show(5);

		Dataset<Row> joinedDF = graphSoftwareDF.join(swhDF, "repoUrl").select("id", "swhid");
//		joinedDF.show(false);

		return joinedDF.map((MapFunction<Row, Software>) row -> {

			Software s = new Software();

			// set openaire id
			s.setId(row.getString(row.fieldIndex("id")));

			// set swh id
			Qualifier qualifier = OafMapperUtils
				.qualifier(
					SWHConstants.SWHID,
					SWHConstants.SWHID_CLASSNAME,
					ModelConstants.DNET_PID_TYPES,
					ModelConstants.DNET_PID_TYPES);

			DataInfo dataInfo = OafMapperUtils
				.dataInfo(
					false,
					null,
					false,
					false,
					ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER,
					"");

			s
				.setPid(
					Arrays
						.asList(
							OafMapperUtils
								.structuredProperty(
									String.format("swh:1:snp:%s", row.getString(row.fieldIndex("swhid"))),
									qualifier,
									dataInfo)));

			// add SWH in the `collectedFrom` field
			KeyValue kv = new KeyValue();
			kv.setKey(SWHConstants.SWH_ID);
			kv.setValue(SWHConstants.SWH_NAME);

			s.setCollectedfrom(Arrays.asList(kv));

			return s;
		}, Encoders.bean(Software.class))
			.toJavaRDD()
			.map(p -> new AtomicAction(Software.class, p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))));
	}
}
