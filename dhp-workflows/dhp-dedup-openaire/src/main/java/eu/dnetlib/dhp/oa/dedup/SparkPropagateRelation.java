
package eu.dnetlib.dhp.oa.dedup;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
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

		final String outputRelationPath = DedupUtility.createEntityPath(graphOutputPath, "relation");
		removeOutputDir(spark, outputRelationPath);

		Dataset<Relation> mergeRels = spark
			.read()
			.load(DedupUtility.createMergeRelPath(workingPath, "*", "*"))
			.as(REL_BEAN_ENC);

		// <mergedObjectID, dedupID>
		Dataset<Row> mergedIds = mergeRels
			.where(col("relClass").equalTo(ModelConstants.MERGES))
			.select(col("source").as("dedupID"), col("target").as("mergedObjectID"))
			.distinct()
			.cache();

		Dataset<Row> allRels = spark
			.read()
			.schema(REL_BEAN_ENC.schema())
			.json(DedupUtility.createEntityPath(graphBasePath, "relation"));

		Dataset<Relation> dedupedRels = allRels
			.joinWith(mergedIds, allRels.col("source").equalTo(mergedIds.col("mergedObjectID")), "left_outer")
			.joinWith(mergedIds, col("_1.target").equalTo(mergedIds.col("mergedObjectID")), "left_outer")
			.select("_1._1", "_1._2.dedupID", "_2.dedupID")
			.as(Encoders.tuple(REL_BEAN_ENC, Encoders.STRING(), Encoders.STRING()))
			.flatMap(SparkPropagateRelation::addInferredRelations, REL_KRYO_ENC);

		Dataset<Relation> processedRelations = distinctRelations(
			dedupedRels.union(mergeRels.map((MapFunction<Relation, Relation>) r -> r, REL_KRYO_ENC)))
				.filter((FilterFunction<Relation>) r -> !Objects.equals(r.getSource(), r.getTarget()));

		save(processedRelations, outputRelationPath, SaveMode.Overwrite);
	}

	private static Iterator<Relation> addInferredRelations(Tuple3<Relation, String, String> t) throws Exception {
		Relation existingRel = t._1();
		String newSource = t._2();
		String newTarget = t._3();

		if (newSource == null && newTarget == null) {
			return Collections.singleton(t._1()).iterator();
		}

		// update existing relation
		if (existingRel.getDataInfo() == null) {
			existingRel.setDataInfo(new DataInfo());
		}
		existingRel.getDataInfo().setDeletedbyinference(true);

		// Create new relation inferred by dedupIDs
		Relation inferredRel = (Relation) BeanUtils.cloneBean(existingRel);

		inferredRel.setDataInfo((DataInfo) BeanUtils.cloneBean(existingRel.getDataInfo()));
		inferredRel.getDataInfo().setDeletedbyinference(false);

		if (newSource != null)
			inferredRel.setSource(newSource);

		if (newTarget != null)
			inferredRel.setTarget(newTarget);

		return Arrays.asList(existingRel, inferredRel).iterator();
	}

	private Dataset<Relation> distinctRelations(Dataset<Relation> rels) {
		return rels
			.filter(getRelationFilterFunction())
			.groupByKey(
				(MapFunction<Relation, String>) r -> String
					.join(" ", r.getSource(), r.getTarget(), r.getRelType(), r.getSubRelType(), r.getRelClass()),
				Encoders.STRING())
			.reduceGroups((ReduceFunction<Relation>) (b, a) -> {
				b.mergeFrom(a);
				return b;
			})
			.map((MapFunction<Tuple2<String, Relation>, Relation>) Tuple2::_2, REL_BEAN_ENC);
	}

	private FilterFunction<Relation> getRelationFilterFunction() {
		return r -> StringUtils.isNotBlank(r.getSource()) ||
			StringUtils.isNotBlank(r.getTarget()) ||
			StringUtils.isNotBlank(r.getRelType()) ||
			StringUtils.isNotBlank(r.getSubRelType()) ||
			StringUtils.isNotBlank(r.getRelClass());
	}
}
