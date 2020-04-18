package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.util.MapDocumentUtil;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkUpdateEntity extends AbstractSparkAction {

    private static final Logger log = LoggerFactory.getLogger(SparkUpdateEntity.class);

    final String IDJSONPATH = "$.id";

    public SparkUpdateEntity(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                SparkUpdateEntity.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json")));
        parser.parseArgument(args);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(ModelSupport.getOafModelClasses());

        new SparkUpdateEntity(parser, getSparkSession(conf))
                .run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    public void run(ISLookUpService isLookUpService) throws IOException {

        final String graphBasePath = parser.get("graphBasePath");
        final String workingPath = parser.get("workingPath");
        final String dedupGraphPath = parser.get("dedupGraphPath");

        log.info("graphBasePath:  '{}'", graphBasePath);
        log.info("workingPath:    '{}'", workingPath);
        log.info("dedupGraphPath: '{}'", dedupGraphPath);

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // for each entity
        ModelSupport.entityTypes.forEach(
                (entity, clazz) -> {
                    final String outputPath = dedupGraphPath + "/" + entity;
                    removeOutputDir(spark, outputPath);

                    JavaRDD<String> sourceEntity =
                            sc.textFile(
                                    DedupUtility.createEntityPath(
                                            graphBasePath, entity.toString()));

                    if (mergeRelExists(workingPath, entity.toString())) {

                        final String mergeRelPath =
                                DedupUtility.createMergeRelPath(
                                        workingPath, "*", entity.toString());
                        final String dedupRecordPath =
                                DedupUtility.createDedupRecordPath(
                                        workingPath, "*", entity.toString());

                        final Dataset<Relation> rel =
                                spark.read().load(mergeRelPath).as(Encoders.bean(Relation.class));

                        final JavaPairRDD<String, String> mergedIds =
                                rel.where("relClass == 'merges'")
                                        .select(rel.col("target"))
                                        .distinct()
                                        .toJavaRDD()
                                        .mapToPair(
                                                (PairFunction<Row, String, String>)
                                                        r -> new Tuple2<>(r.getString(0), "d"));

                        JavaPairRDD<String, String> entitiesWithId =
                                sourceEntity.mapToPair(
                                        (PairFunction<String, String, String>)
                                                s ->
                                                        new Tuple2<>(
                                                                MapDocumentUtil.getJPathString(
                                                                        IDJSONPATH, s),
                                                                s));

                        JavaRDD<String> map =
                                entitiesWithId
                                        .leftOuterJoin(mergedIds)
                                        .map(
                                                k ->
                                                        k._2()._2().isPresent()
                                                                ? updateDeletedByInference(
                                                                        k._2()._1(), clazz)
                                                                : k._2()._1());

                        sourceEntity = map.union(sc.textFile(dedupRecordPath));
                    }

                    sourceEntity.saveAsTextFile(outputPath, GzipCodec.class);
                });
    }

    public boolean mergeRelExists(String basePath, String entity) {

        boolean result = false;
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());

            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(basePath));

            for (FileStatus fs : fileStatuses) {
                if (fs.isDirectory())
                    if (fileSystem.exists(
                            new Path(
                                    DedupUtility.createMergeRelPath(
                                            basePath, fs.getPath().getName(), entity))))
                        result = true;
            }

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T extends OafEntity> String updateDeletedByInference(
            final String json, final Class<T> clazz) {
        try {
            Oaf entity = OBJECT_MAPPER.readValue(json, clazz);
            if (entity.getDataInfo() == null) entity.setDataInfo(new DataInfo());
            entity.getDataInfo().setDeletedbyinference(true);
            return OBJECT_MAPPER.writeValueAsString(entity);
        } catch (IOException e) {
            throw new RuntimeException("Unable to convert json", e);
        }
    }
}
