
package eu.dnetlib.dhp.incremental;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static org.apache.spark.sql.functions.udf;

import java.util.Collections;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.aggregation.mdstore.MDStoreActionNode;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.rest.DNetRestClient;
import eu.dnetlib.dhp.oa.graph.raw.CopyHdfsOafSparkApplication;
import eu.dnetlib.dhp.oozie.RunSQLSparkJob;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion;
import scala.collection.JavaConversions;

public class CollectNewOafResults {
	private static final Logger log = LoggerFactory.getLogger(RunSQLSparkJob.class);

	private final ArgumentApplicationParser parser;

	public CollectNewOafResults(ArgumentApplicationParser parser) {
		this.parser = parser;
	}

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				CollectNewOafResults.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/incremental/collect/collectnewresults_input_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String wrkdirPath = parser.get("workingDir");
		log.info("workingDir is {}", wrkdirPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath is {}", outputPath);

		final String mdStoreManagerURI = parser.get("mdStoreManagerURI");
		log.info("mdStoreManagerURI is {}", mdStoreManagerURI);

		final String mdStoreID = parser.get("mdStoreID");
		if (StringUtils.isBlank(mdStoreID)) {
			throw new IllegalArgumentException("missing or empty argument mdStoreID");
		}

		final String hiveDbName = parser.get("hiveDbName");
		log.info("hiveDbName is {}", hiveDbName);

		final MDStoreVersion currentVersion = DNetRestClient
			.doGET(String.format(MDStoreActionNode.READ_LOCK_URL, mdStoreManagerURI, mdStoreID), MDStoreVersion.class);

		log.info("mdstore data is {}", currentVersion.toString());

		try {

			SparkConf conf = new SparkConf();
			conf.set("hive.metastore.uris", parser.get("hiveMetastoreUris"));

			runWithSparkHiveSession(
				conf,
				isSparkSessionManaged,
				spark -> {
					// ids in the current graph
					Dataset<Row> currentIds = spark
						.table(hiveDbName + ".result")
						.select("id")
						.union(
							spark
								.table(hiveDbName + ".relation")
								.where("relClass = 'merges'")
								.selectExpr("target as id"))
						.distinct();

					UserDefinedFunction getOafType = udf(
						(String json) -> CopyHdfsOafSparkApplication.getOafType(json), DataTypes.StringType);

					// new collected ids
					spark
						.read()
						.text(currentVersion.getHdfsPath() + "/store")
						.selectExpr(
							"value",
							"get_json_object(value, '$.id') AS id")
						.where("id IS NOT NULL")
						.join(currentIds, JavaConversions.asScalaBuffer(Collections.singletonList("id")), "left_anti")
						.withColumn("oaftype", getOafType.apply(new Column("value")))
						.write()
						.partitionBy("oaftype")
						.mode(SaveMode.Overwrite)
						.option("compression", "gzip")
						.parquet(wrkdirPath + "/entities");

					ModelSupport.oafTypes
						.keySet()
						.forEach(
							entity -> spark
								.read()
								.parquet(wrkdirPath + "/entities")
								.filter("oaftype = '" + entity + "'")
								.select("value")
								.write()
								.option("compression", "gzip")
								.mode(SaveMode.Append)
								.text(outputPath + "/" + entity));

					Dataset<Row> newIds = spark.read().parquet(wrkdirPath + "/entities").select("id");

					Dataset<Row> rels = spark
						.read()
						.text(currentVersion.getHdfsPath() + "/store")
						.selectExpr(
							"value",
							"get_json_object(value, '$.source') AS source",
							"get_json_object(value, '$.target') AS target")
						.where("source IS NOT NULL AND target IS NOT NULL");

					rels
						.join(
							newIds.selectExpr("id as source"),
							JavaConversions.asScalaBuffer(Collections.singletonList("source")), "left_semi")
						.union(
							rels
								.join(
									newIds.selectExpr("id as target"),
									JavaConversions.asScalaBuffer(Collections.singletonList("target")), "left_semi"))
						.distinct()
						.select("value")
						.write()
						.option("compression", "gzip")
						.mode(SaveMode.Append)
						.text(outputPath + "/relation");
				});
		} finally {
			DNetRestClient
				.doGET(String.format(MDStoreActionNode.READ_UNLOCK_URL, mdStoreManagerURI, currentVersion.getId()));
		}
	}
}
