
package eu.dnetlib.dhp.oa.dedup;

import static org.apache.spark.sql.functions.col;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;
import scala.Tuple3;

public class SparkPropagateRelation extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkPropagateRelation.class);

	private static Encoder<Relation> REL_BEAN_ENC = Encoders.bean(Relation.class);

	private static Encoder<Relation> REL_KRYO_ENC = Encoders.kryo(Relation.class);

	public SparkPropagateRelation(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkPropagateRelation.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/propagateRelation_parameters.json")));

		parser.parseArgument(args);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		new SparkPropagateRelation(parser, getSparkSession(conf))
			.run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
	}

	@Override
	public void run(ISLookUpService isLookUpService) {

		final String graphBasePath = parser.get("graphBasePath");
		final String workingPath = parser.get("workingPath");
		final String graphOutputPath = parser.get("graphOutputPath");

		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("workingPath: '{}'", workingPath);
		log.info("graphOutputPath: '{}'", graphOutputPath);

		Dataset<Relation> mergeRels = spark
			.read()
			.load(DedupUtility.createMergeRelPath(workingPath, "*", "*"))
			.as(REL_BEAN_ENC);

		// <mergedObjectID, dedupID>
		Dataset<Row> idsToMerge = mergeRels
			.where(col("relClass").equalTo(ModelConstants.MERGES))
			.select(col("source").as("dedupID"), col("target").as("mergedObjectID"))
			.distinct();

		Dataset<Row> allRels = spark
			.read()
			.schema(REL_BEAN_ENC.schema())
			.json(graphBasePath + "/relation");

		Dataset<Relation> dedupedRels = allRels
			.joinWith(idsToMerge, allRels.col("source").equalTo(idsToMerge.col("mergedObjectID")), "left_outer")
			.joinWith(idsToMerge, col("_1.target").equalTo(idsToMerge.col("mergedObjectID")), "left_outer")
			.select("_1._1", "_1._2.dedupID", "_2.dedupID")
			.as(Encoders.tuple(REL_BEAN_ENC, Encoders.STRING(), Encoders.STRING()))
			.map((MapFunction<Tuple3<Relation, String, String>, Relation>) t -> {
				Relation rel = t._1();
				String newSource = t._2();
				String newTarget = t._3();

				if (rel.getDataInfo() == null) {
					rel.setDataInfo(new DataInfo());
				}

				if (newSource != null || newTarget != null) {
					rel.getDataInfo().setDeletedbyinference(false);

					if (newSource != null)
						rel.setSource(newSource);

					if (newTarget != null)
						rel.setTarget(newTarget);
				}

				return rel;
			}, REL_BEAN_ENC);

		// ids of records that are both not deletedbyinference and not invisible
		Dataset<Row> ids = validIds(spark, graphBasePath);

		// filter relations that point to valid records, can force them to be visible
		Dataset<Relation> cleanedRels = dedupedRels
			.join(ids, col("source").equalTo(ids.col("id")), "leftsemi")
			.join(ids, col("target").equalTo(ids.col("id")), "leftsemi")
			.as(REL_BEAN_ENC)
			.map((MapFunction<Relation, Relation>) r -> {
				r.getDataInfo().setInvisible(false);
				return r;
			}, REL_KRYO_ENC);

		Dataset<Relation> distinctRels = cleanedRels
			.groupByKey(
				(MapFunction<Relation, String>) r -> String
					.join(" ", r.getSource(), r.getTarget(), r.getRelType(), r.getSubRelType(), r.getRelClass()),
				Encoders.STRING())
			.reduceGroups((ReduceFunction<Relation>) MergeUtils::mergeRelation)
			.map((MapFunction<Tuple2<String, Relation>, Relation>) Tuple2::_2, REL_BEAN_ENC);

		final String outputRelationPath = graphOutputPath + "/relation";
		removeOutputDir(spark, outputRelationPath);
		save(
			distinctRels
				.union(mergeRels)
				.filter("source != target AND dataInfo.deletedbyinference != true AND dataInfo.invisible != true"),
			outputRelationPath,
			SaveMode.Overwrite);
	}

	static Dataset<Row> validIds(SparkSession spark, String graphBasePath) {
		StructType idsSchema = StructType
			.fromDDL("`id` STRING, `dataInfo` STRUCT<`deletedbyinference`:BOOLEAN,`invisible`:BOOLEAN>");

		Dataset<Row> allIds = spark.emptyDataset(RowEncoder.apply(idsSchema));

		for (EntityType entityType : ModelSupport.entityTypes.keySet()) {
			String entityPath = graphBasePath + '/' + entityType.name();
			if (HdfsSupport.exists(entityPath, spark.sparkContext().hadoopConfiguration())) {
				allIds = allIds.union(spark.read().schema(idsSchema).json(entityPath));
			}
		}

		return allIds
			.filter("dataInfo.deletedbyinference != true AND dataInfo.invisible != true")
			.select("id")
			.distinct();
	}
}
