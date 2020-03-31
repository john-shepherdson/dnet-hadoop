package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;

public class SparkUpdateEntity implements Serializable {

    final String IDJSONPATH = "$.id";

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkUpdateEntity.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json")));
        parser.parseArgument(args);

        new SparkUpdateEntity().run(parser);
    }

    public boolean mergeRelExists(String basePath, String entity) throws IOException {

        boolean result = false;

        FileSystem fileSystem = FileSystem.get(new Configuration());

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(basePath));

        for (FileStatus fs : fileStatuses) {
            if (fs.isDirectory())
                if (fileSystem.exists(new Path(DedupUtility.createMergeRelPath(basePath, fs.getPath().getName(), entity))))
                    result = true;
        }

        return result;
    }

    public void run(ArgumentApplicationParser parser) throws IOException {

        final String graphBasePath = parser.get("graphBasePath");
        final String workingPath = parser.get("workingPath");
        final String dedupGraphPath = parser.get("dedupGraphPath");

        try (SparkSession spark = getSparkSession(parser)) {

            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            //for each entity
            for (OafEntityType entity: OafEntityType.values()) {

                JavaRDD<String> sourceEntity = sc.textFile(DedupUtility.createEntityPath(graphBasePath, entity.toString()));

                if (mergeRelExists(workingPath, entity.toString())) {

                    final Dataset<Relation> rel = spark.read().load(DedupUtility.createMergeRelPath(workingPath, "*", entity.toString())).as(Encoders.bean(Relation.class));

                    final JavaPairRDD<String, String> mergedIds = rel
                            .where("relClass == 'merges'")
                            .select(rel.col("target"))
                            .distinct()
                            .toJavaRDD()
                            .mapToPair((PairFunction<Row, String, String>) r -> new Tuple2<>(r.getString(0), "d"));

                    final JavaRDD<String> dedupEntity = sc.textFile(DedupUtility.createDedupRecordPath(workingPath, "*", entity.toString()));

                    JavaPairRDD<String, String> entitiesWithId = sourceEntity.mapToPair((PairFunction<String, String, String>) s -> new Tuple2<>(MapDocumentUtil.getJPathString(IDJSONPATH, s), s));

                    JavaRDD<String> map = entitiesWithId.leftOuterJoin(mergedIds).map(k -> k._2()._2().isPresent() ? updateDeletedByInference(k._2()._1(), getOafClass(entity)) : k._2()._1());
                    sourceEntity = map.union(dedupEntity);

                }

                sourceEntity.saveAsTextFile(dedupGraphPath + "/" + entity, GzipCodec.class);

            }
        }
    }

    public Class<? extends Oaf> getOafClass(OafEntityType className) {
        switch (className.toString()) {
            case "publication":
                return Publication.class;
            case "dataset":
                return eu.dnetlib.dhp.schema.oaf.Dataset.class;
            case "datasource":
                return Datasource.class;
            case "software":
                return Software.class;
            case "organization":
                return Organization.class;
            case "otherresearchproduct":
                return OtherResearchProduct.class;
            case "project":
                return Project.class;
            default:
                throw new IllegalArgumentException("Illegal type " + className);
        }
    }

    private static <T extends Oaf> String updateDeletedByInference(final String json, final Class<T> clazz) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            Oaf entity = mapper.readValue(json, clazz);
            if (entity.getDataInfo()== null)
                entity.setDataInfo(new DataInfo());
            entity.getDataInfo().setDeletedbyinference(true);
            return mapper.writeValueAsString(entity);
        } catch (IOException e) {
            throw new RuntimeException("Unable to convert json", e);
        }
    }

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession
                .builder()
                .appName(SparkUpdateEntity.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }
}
