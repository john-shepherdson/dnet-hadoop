package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;

public class SparkPropagateRelation extends AbstractSparkAction {

    private static final Logger log = LoggerFactory.getLogger(SparkPropagateRelation.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    enum FieldType {
        SOURCE,
        TARGET
    }

    public SparkPropagateRelation(ArgumentApplicationParser parser, SparkSession spark) throws Exception {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(SparkCreateSimRels.class.getResourceAsStream(
                        "/eu/dnetlib/dhp/oa/dedup/propagateRelation_parameters.json")));

        parser.parseArgument(args);

        new SparkPropagateRelation(parser, getSparkSession(parser))
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
        deletePath(outputRelationPath);

        Dataset<Relation> mergeRels = spark.read()
                .load(DedupUtility.createMergeRelPath(workingPath, "*", "*"))
                .as(Encoders.bean(Relation.class));

        Dataset<Tuple2<String, String>> mergedIds = mergeRels
                .where(col("relClass").equalTo("merges"))
                .select(col("source"), col("target"))
                .distinct()
                .map((MapFunction<Row, Tuple2<String, String>>)
                        r -> new Tuple2<>(r.getString(1), r.getString(0)),
                        Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .cache();

        final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

        Dataset<Relation> rels = spark.read()
                .textFile(relationPath)
                .map(patchRelFn(), Encoders.bean(Relation.class));

        //change raw ids with dedup ids
        Dataset<Relation> newRels =
                processDataset(
                    processDataset(rels, mergedIds, FieldType.SOURCE, getFixRelFn(FieldType.SOURCE)),
                mergedIds, FieldType.TARGET, getFixRelFn(FieldType.TARGET))
                .filter(SparkPropagateRelation::containsDedup);

        //update deletedbyinference
        Dataset<Relation> updated = processDataset(
                processDataset(rels, mergedIds, FieldType.SOURCE, getDeletedFn()),
                mergedIds, FieldType.TARGET, getDeletedFn());

        save(newRels.union(updated), outputRelationPath);

    }

    private static Dataset<Relation> processDataset(Dataset<Relation> rels, Dataset<Tuple2<String, String>> mergedIds, FieldType type,
                                                    MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, String>>, Relation> mapFn) {
        final Dataset<Tuple2<String, Relation>> mapped = rels
                .map((MapFunction<Relation, Tuple2<String, Relation>>)
                                r -> new Tuple2<>(getId(r, type), r),
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

    private static MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, String>>, Relation> getFixRelFn(FieldType type) {
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

    private void deletePath(String path) {
        try {
            Path p = new Path(path);
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

            if (fs.exists(p)) {
                fs.delete(p, true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void save(Dataset<Relation> dataset, String outPath) {
        dataset
                .write()
                .option("compression", "gzip")
                .json(outPath);
    }

    private static boolean containsDedup(final Relation r) {
        return r.getSource().toLowerCase().contains("dedup") || r.getTarget().toLowerCase().contains("dedup");
    }

}