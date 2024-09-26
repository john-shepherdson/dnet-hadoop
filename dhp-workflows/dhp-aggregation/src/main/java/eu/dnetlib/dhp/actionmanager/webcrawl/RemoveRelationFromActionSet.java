
package eu.dnetlib.dhp.actionmanager.webcrawl;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.*;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import scala.Tuple2;

public class RemoveRelationFromActionSet
	implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(CreateActionSetFromWebEntries.class);

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final StructType KV_SCHEMA = StructType$.MODULE$
		.apply(
			Arrays
				.asList(
					StructField$.MODULE$.apply("key", DataTypes.StringType, false, Metadata.empty()),
					StructField$.MODULE$.apply("value", DataTypes.StringType, false, Metadata.empty())));

	private static final StructType ATOMIC_ACTION_SCHEMA = StructType$.MODULE$
		.apply(
			Arrays
				.asList(
					StructField$.MODULE$.apply("clazz", DataTypes.StringType, false, Metadata.empty()),
					StructField$.MODULE$
						.apply(
							"payload", DataTypes.StringType, false, Metadata.empty())));

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				CreateActionSetFromWebEntries.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/webcrawl/as_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		// the actionSet path
		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String blackListInputPath = parser.get("blackListPath");
		log.info("blackListInputPath: {}", blackListInputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {

				removeFromActionSet(spark, inputPath, outputPath, blackListInputPath);

			});
	}

	private static void removeFromActionSet(SparkSession spark, String inputPath, String outputPath,
		String blackListInputPath) {
		// read the blacklist
		Dataset<String> blackList = readBlackList(spark, blackListInputPath)
			.map(
				(MapFunction<Row, String>) r -> IdentifierFactory
					.idFromPid("50", "doi", ((String) r.getAs("doi")).substring(16), true),
				Encoders.STRING());

		// read the old actionset and get the relations in the payload
		JavaPairRDD<Text, Text> seq = JavaSparkContext
			.fromSparkContext(spark.sparkContext())
			.sequenceFile(inputPath, Text.class, Text.class);

		JavaRDD<Row> rdd = seq
			.map(x -> RowFactory.create(x._1().toString(), x._2().toString()));

		Dataset<Row> actionSet = spark
			.createDataFrame(rdd, KV_SCHEMA)
			.withColumn("atomic_action", from_json(col("value"), ATOMIC_ACTION_SCHEMA))
			.select(expr("atomic_action.*"));

		Dataset<Relation> relation = actionSet
			.map(
				(MapFunction<Row, Relation>) r -> MAPPER.readValue((String) r.getAs("payload"), Relation.class),
				Encoders.bean(Relation.class));

		// select only the relation not matching any pid in the blacklist as source for the relation
		Dataset<Relation> relNoSource = relation
			.joinWith(blackList, relation.col("source").equalTo(blackList.col("value")), "left")
			.filter((FilterFunction<Tuple2<Relation, String>>) t2 -> t2._2() == null)
			.map((MapFunction<Tuple2<Relation, String>, Relation>) t2 -> t2._1(), Encoders.bean(Relation.class));

		// select only the relation not matching any pid in the blacklist as target of the relation
		relNoSource
			.joinWith(blackList, relNoSource.col("target").equalTo(blackList.col("value")), "left")
			.filter((FilterFunction<Tuple2<Relation, String>>) t2 -> t2._2() == null)
			.map((MapFunction<Tuple2<Relation, String>, Relation>) t2 -> t2._1(), Encoders.bean(Relation.class))
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, BZip2Codec.class);
		;

	}

	private static Dataset<Row> readBlackList(SparkSession spark, String inputPath) {

		return spark
			.read()
			.json(inputPath)
			.select("doi");
	}

}
