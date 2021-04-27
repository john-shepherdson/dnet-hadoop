
package eu.dnetlib.dhp.oa.dedup;

import static org.apache.spark.sql.functions.col;

import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
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

	enum FieldType {
		SOURCE, TARGET
	}

	public SparkPropagateRelation(ArgumentApplicationParser parser, SparkSession spark) {
		super(parser, spark);
	}

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
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
			.as(Encoders.bean(Relation.class));

		// <mergedObjectID, dedupID>
		Dataset<Tuple2<String, String>> mergedIds = mergeRels
			.where(col("relClass").equalTo(ModelConstants.MERGES))
			.select(col("source"), col("target"))
			.distinct()
			.map(
				(MapFunction<Row, Tuple2<String, String>>) r -> new Tuple2<>(r.getString(1), r.getString(0)),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
			.cache();

		final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

		Dataset<Relation> rels = spark.read().textFile(relationPath).map(patchRelFn(), Encoders.bean(Relation.class));

		Dataset<Relation> newRels = createNewRels(rels, mergedIds, getFixRelFn());

		Dataset<Relation> updated = processDataset(
			processDataset(rels, mergedIds, FieldType.SOURCE, getDeletedFn()),
			mergedIds,
			FieldType.TARGET,
			getDeletedFn());

		save(
			distinctRelations(
				newRels
					.union(updated)
					.union(mergeRels)
					.map((MapFunction<Relation, Relation>) r -> r, Encoders.kryo(Relation.class)))
						.filter((FilterFunction<Relation>) r -> !Objects.equals(r.getSource(), r.getTarget())),
			outputRelationPath, SaveMode.Overwrite);
	}

	private Dataset<Relation> distinctRelations(Dataset<Relation> rels) {
		return rels
			.filter(getRelationFilterFunction())
			.groupByKey(
				(MapFunction<Relation, String>) r -> String
					.join(r.getSource(), r.getTarget(), r.getRelType(), r.getSubRelType(), r.getRelClass()),
				Encoders.STRING())
			.agg(new RelationAggregator().toColumn())
			.map((MapFunction<Tuple2<String, Relation>, Relation>) t -> t._2(), Encoders.bean(Relation.class));
	}

	// redirect the relations to the dedupID
	private static Dataset<Relation> createNewRels(
		Dataset<Relation> rels, // all the relations to be redirected
		Dataset<Tuple2<String, String>> mergedIds, // merge rels: <mergedObjectID, dedupID>
		MapFunction<Tuple2<Tuple2<Tuple3<String, Relation, String>, Tuple2<String, String>>, Tuple2<String, String>>, Relation> mapRel) {

		// <sourceID, relation, targetID>
		Dataset<Tuple3<String, Relation, String>> mapped = rels
			.map(
				(MapFunction<Relation, Tuple3<String, Relation, String>>) r -> new Tuple3<>(getId(r, FieldType.SOURCE),
					r, getId(r, FieldType.TARGET)),
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(Relation.class), Encoders.STRING()));

		// < <sourceID, relation, target>, <sourceID, dedupID> >
		Dataset<Tuple2<Tuple3<String, Relation, String>, Tuple2<String, String>>> relSource = mapped
			.joinWith(mergedIds, mapped.col("_1").equalTo(mergedIds.col("_1")), "left_outer");

		// < <<sourceID, relation, targetID>, <sourceID, dedupID>>, <targetID, dedupID> >
		Dataset<Tuple2<Tuple2<Tuple3<String, Relation, String>, Tuple2<String, String>>, Tuple2<String, String>>> relSourceTarget = relSource
			.joinWith(mergedIds, relSource.col("_1._3").equalTo(mergedIds.col("_1")), "left_outer");

		return relSourceTarget
			.filter(
				(FilterFunction<Tuple2<Tuple2<Tuple3<String, Relation, String>, Tuple2<String, String>>, Tuple2<String, String>>>) r -> r
					._1()
					._1() != null || r._2() != null)
			.map(mapRel, Encoders.bean(Relation.class))
			.distinct();
	}

	private static Dataset<Relation> processDataset(
		Dataset<Relation> rels,
		Dataset<Tuple2<String, String>> mergedIds,
		FieldType type,
		MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, String>>, Relation> mapFn) {
		final Dataset<Tuple2<String, Relation>> mapped = rels
			.map(
				(MapFunction<Relation, Tuple2<String, Relation>>) r -> new Tuple2<>(getId(r, type), r),
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(Relation.class)));
		return mapped
			.joinWith(mergedIds, mapped.col("_1").equalTo(mergedIds.col("_1")), "left_outer")
			.map(mapFn, Encoders.bean(Relation.class));
	}

	private FilterFunction<Relation> getRelationFilterFunction() {
		return (FilterFunction<Relation>) r -> StringUtils.isNotBlank(r.getSource()) ||
			StringUtils.isNotBlank(r.getTarget()) ||
			StringUtils.isNotBlank(r.getRelClass()) ||
			StringUtils.isNotBlank(r.getSubRelType()) ||
			StringUtils.isNotBlank(r.getRelClass());
	}

	private static String getId(Relation r, FieldType type) {
		switch (type) {
			case SOURCE:
				return r.getSource();
			case TARGET:
				return r.getTarget();
			default:
				throw new IllegalArgumentException("");
		}
	}

	private static MapFunction<Tuple2<Tuple2<Tuple3<String, Relation, String>, Tuple2<String, String>>, Tuple2<String, String>>, Relation> getFixRelFn() {
		return value -> {

			Relation r = value._1()._1()._2();
			String newSource = value._1()._2() != null ? value._1()._2()._2() : null;
			String newTarget = value._2() != null ? value._2()._2() : null;

			if (r.getDataInfo() == null) {
				r.setDataInfo(new DataInfo());
			}
			r.getDataInfo().setDeletedbyinference(false);

			if (newSource != null)
				r.setSource(newSource);

			if (newTarget != null)
				r.setTarget(newTarget);

			return r;
		};
	}

	private static MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, String>>, Relation> getDeletedFn() {
		return value -> {
			if (value._2() != null) {
				Relation r = value._1()._2();
				if (r.getDataInfo() == null) {
					r.setDataInfo(new DataInfo());
				}
				r.getDataInfo().setDeletedbyinference(true);
				return r;
			}
			return value._1()._2();
		};
	}

}
