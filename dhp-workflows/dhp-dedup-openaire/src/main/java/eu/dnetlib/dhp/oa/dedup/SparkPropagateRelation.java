
package eu.dnetlib.dhp.oa.dedup;

import static org.apache.spark.sql.functions.col;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkPropagateRelation extends AbstractSparkAction {

	private static final Logger log = LoggerFactory.getLogger(SparkPropagateRelation.class);

	enum FieldType {
		SOURCE, TARGET
	}

	public SparkPropagateRelation(ArgumentApplicationParser parser, SparkSession spark)
		throws Exception {
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
		final String dedupGraphPath = parser.get("dedupGraphPath");

		log.info("graphBasePath: '{}'", graphBasePath);
		log.info("workingPath: '{}'", workingPath);
		log.info("dedupGraphPath: '{}'", dedupGraphPath);

		final String outputRelationPath = DedupUtility.createEntityPath(dedupGraphPath, "relation");
		removeOutputDir(spark, outputRelationPath);

		Dataset<Relation> mergeRels = spark
			.read()
			.load(DedupUtility.createMergeRelPath(workingPath, "*", "*"))
			.as(Encoders.bean(Relation.class));

		Dataset<Tuple2<String, String>> mergedIds = mergeRels
			.where(col("relClass").equalTo("merges"))
			.select(col("source"), col("target"))
			.distinct()
			.map(
				(MapFunction<Row, Tuple2<String, String>>) r -> new Tuple2<>(r.getString(1), r.getString(0)),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
			.cache();

		final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

		Dataset<Relation> rels = spark.read().textFile(relationPath).map(patchRelFn(), Encoders.bean(Relation.class));

		Dataset<Relation> newRels = processDataset(
			processDataset(rels, mergedIds, FieldType.SOURCE, getFixRelFn(FieldType.SOURCE)),
			mergedIds,
			FieldType.TARGET,
			getFixRelFn(FieldType.TARGET))
				.filter(SparkPropagateRelation::containsDedup);

		Dataset<Relation> updated = processDataset(
			processDataset(rels, mergedIds, FieldType.SOURCE, getDeletedFn()),
			mergedIds,
			FieldType.TARGET,
			getDeletedFn());

		save(newRels.union(updated), outputRelationPath, SaveMode.Overwrite);
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

	private static MapFunction<String, Relation> patchRelFn() {
		return value -> {
			final Relation rel = OBJECT_MAPPER.readValue(value, Relation.class);
			if (rel.getDataInfo() == null) {
				rel.setDataInfo(new DataInfo());
			}
			return rel;
		};
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

	private static MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, String>>, Relation> getFixRelFn(
		FieldType type) {
		return value -> {
			if (value._2() != null) {
				Relation r = value._1()._2();
				String id = value._2()._2();
				if (r.getDataInfo() == null) {
					r.setDataInfo(new DataInfo());
				}
				r.getDataInfo().setDeletedbyinference(false);
				switch (type) {
					case SOURCE:
						r.setSource(id);
						return r;
					case TARGET:
						r.setTarget(id);
						return r;
					default:
						throw new IllegalArgumentException("");
				}
			}
			return value._1()._2();
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

	private static boolean containsDedup(final Relation r) {
		return r.getSource().toLowerCase().contains("dedup")
			|| r.getTarget().toLowerCase().contains("dedup");
	}
}
