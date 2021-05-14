
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.broker.objects.OaBrokerRelatedDatasource;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.DatasourceRelationsAccumulator;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedDatasource;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import scala.Tuple2;
import scala.Tuple3;

public class PrepareRelatedDatasourcesJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareRelatedDatasourcesJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PrepareRelatedDatasourcesJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/common_params.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String graphPath = parser.get("graphPath");
		log.info("graphPath: {}", graphPath);

		final String workingDir = parser.get("workingDir");
		log.info("workingDir: {}", workingDir);

		final String relsPath = workingDir + "/relatedDatasources";
		log.info("relsPath: {}", relsPath);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, relsPath);

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_datasources");

			final Dataset<Tuple3<String, String, String>> rels = prepareResultTuples(
				spark, graphPath, Publication.class)
					.union(prepareResultTuples(spark, graphPath, eu.dnetlib.dhp.schema.oaf.Dataset.class))
					.union(prepareResultTuples(spark, graphPath, Software.class))
					.union(prepareResultTuples(spark, graphPath, OtherResearchProduct.class));

			final Dataset<OaBrokerRelatedDatasource> datasources = ClusterUtils
				.readPath(spark, graphPath + "/datasource", Datasource.class)
				.map(
					(MapFunction<Datasource, OaBrokerRelatedDatasource>) ConversionUtils::oafDatasourceToBrokerDatasource,
					Encoders.bean(OaBrokerRelatedDatasource.class));

			final Dataset<RelatedDatasource> dataset = rels
				.joinWith(datasources, datasources.col("openaireId").equalTo(rels.col("_2")), "inner")
				.map(
					(MapFunction<Tuple2<Tuple3<String, String, String>, OaBrokerRelatedDatasource>, RelatedDatasource>) t -> {
						final RelatedDatasource r = new RelatedDatasource();
						r.setSource(t._1._1());
						r.setRelDatasource(t._2);
						r.getRelDatasource().setRelType(t._1._3());
						return r;
					}, Encoders.bean(RelatedDatasource.class));

			ClusterUtils.save(dataset, relsPath, RelatedDatasource.class, total);

		});

	}

	private static final <T extends Result> Dataset<Tuple3<String, String, String>> prepareResultTuples(
		final SparkSession spark,
		final String graphPath,
		final Class<T> sourceClass) {

		return ClusterUtils
			.readPath(spark, graphPath + "/" + sourceClass.getSimpleName().toLowerCase(), sourceClass)
			.filter((FilterFunction<T>) r -> !ClusterUtils.isDedupRoot(r.getId()))
			.filter((FilterFunction<T>) r -> r.getDataInfo().getDeletedbyinference())
			.map(
				(MapFunction<T, DatasourceRelationsAccumulator>) r -> DatasourceRelationsAccumulator.calculateTuples(r),
				Encoders.bean(DatasourceRelationsAccumulator.class))
			.flatMap(
				(FlatMapFunction<DatasourceRelationsAccumulator, Tuple3<String, String, String>>) acc -> acc
					.getRels()
					.iterator(),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));
	}

}
