package eu.dnetlib.dedup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.scholexplorer.DLIDataset;
import eu.dnetlib.dhp.schema.scholexplorer.DLIPublication;
import eu.dnetlib.dhp.schema.scholexplorer.DLIUnknown;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.IOException;

public class SparkUpdateEntityJob {

    final static String IDJSONPATH = "$.id";

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkUpdateEntityJob.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/dedup_delete_by_inference_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkUpdateEntityJob.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String entityPath = parser.get("entityPath");
        final String mergeRelPath = parser.get("mergeRelPath");
        final String dedupRecordPath = parser.get("dedupRecordPath");
        final String entity = parser.get("entity");
        final String destination = parser.get("targetPath");

        final Dataset<Relation> df = spark.read().load(mergeRelPath).as(Encoders.bean(Relation.class));
        final JavaPairRDD<String, String> mergedIds = df
                .where("relClass == 'merges'")
                .select(df.col("target"))
                .distinct()
                .toJavaRDD()
                .mapToPair((PairFunction<Row, String, String>) r -> new Tuple2<>(r.getString(0), "d"));
        final JavaRDD<String> sourceEntity = sc.textFile(entityPath);

        final JavaRDD<String> dedupEntity = sc.textFile(dedupRecordPath);
            JavaPairRDD<String, String> entitiesWithId = sourceEntity.mapToPair((PairFunction<String, String, String>) s -> new Tuple2<>(DHPUtils.getJPathString(IDJSONPATH, s), s));
            Class<? extends Oaf> mainClass;
            switch (entity) {
                case "publication":
                    mainClass = DLIPublication.class;
                    break;
                case "dataset":
                    mainClass = DLIDataset.class;
                    break;
                case "unknown":
                    mainClass = DLIUnknown.class;
                    break;
                default:
                    throw new IllegalArgumentException("Illegal type " + entity);

            }
            JavaRDD<String> map = entitiesWithId.leftOuterJoin(mergedIds).map(k -> k._2()._2().isPresent() ? updateDeletedByInference(k._2()._1(), mainClass) : k._2()._1());
            map.union(dedupEntity).saveAsTextFile(destination, GzipCodec.class);

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


}
