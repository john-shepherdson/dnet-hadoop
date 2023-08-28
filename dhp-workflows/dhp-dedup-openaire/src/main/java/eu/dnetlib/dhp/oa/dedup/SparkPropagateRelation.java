
package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Objects;

import static org.apache.spark.sql.functions.col;

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

        final String inputRelationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

        Dataset<Relation> rels = spark.read().schema(REL_BEAN_ENC.schema()).json(inputRelationPath)
                .as(REL_BEAN_ENC)
//                .map((MapFunction<Relation, Relation>) rel -> {
//                    if (rel.getDataInfo() == null) {
//                        rel.setDataInfo(new DataInfo());
//                    }
//                    return rel;
//                }, REL_BEAN_ENC)
                ;

        Dataset<Tuple3<Relation, String, String>> dedupedRels = rels
                .joinWith(mergedIds, rels.col("source").equalTo(mergedIds.col("mergedObjectID")), "left_outer")
                .joinWith(mergedIds, col("_1.target").equalTo(mergedIds.col("mergedObjectID")), "left_outer")
                .filter("_1._2 IS NOT NULL OR _2 IS NOT NULL")
                .select("_1._1", "_1._2.dedupID", "_2.dedupID")
                .as(Encoders.tuple(REL_BEAN_ENC, Encoders.STRING(), Encoders.STRING()))
                .cache();

        mergedIds.unpersist();

        Dataset<Relation> newRels = dedupedRels
                .map((MapFunction<Tuple3<Relation, String, String>, Relation>) t -> {
                    Relation r = t._1();
                    String newSource = t._2();
                    String newTarget = t._3();

                    if (r.getDataInfo() == null) {
                        r.setDataInfo(new DataInfo());
                    }
                    r.getDataInfo().setDeletedbyinference(false);

                    if (newSource != null)
                        r.setSource(newSource);

                    if (newTarget != null)
                        r.setTarget(newTarget);

                    return r;
                }, REL_BEAN_ENC)
                .distinct();

        Dataset<Relation> updated = dedupedRels
                .map((MapFunction<Tuple3<Relation, String, String>, Relation>) t -> {
                    Relation r = t._1();
                    if (r.getDataInfo() == null) {
                        r.setDataInfo(new DataInfo());
                    }
                    r.getDataInfo().setDeletedbyinference(true);
                    return r;
                }, REL_BEAN_ENC);

        save(
                distinctRelations(
                        newRels
                                .union(updated)
                                .union(mergeRels)
                                .map((MapFunction<Relation, Relation>) r -> r, REL_KRYO_ENC)
                )
                        .filter((FilterFunction<Relation>) r -> !Objects.equals(r.getSource(), r.getTarget())),
                outputRelationPath, SaveMode.Overwrite);
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
                        }
                )

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
