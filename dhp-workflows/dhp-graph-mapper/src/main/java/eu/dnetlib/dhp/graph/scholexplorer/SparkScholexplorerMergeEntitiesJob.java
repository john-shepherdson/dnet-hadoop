package eu.dnetlib.dhp.graph.scholexplorer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.graph.SparkGraphImporterJob;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.scholexplorer.DLIDataset;
import eu.dnetlib.dhp.schema.scholexplorer.DLIPublication;
import eu.dnetlib.dhp.schema.scholexplorer.DLIUnknown;
import eu.dnetlib.dhp.utils.DHPUtils;
import net.minidev.json.JSONArray;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SparkScholexplorerMergeEntitiesJob {

    final static String IDJSONPATH = "$.id";
    final static String SOURCEJSONPATH = "$.source";
    final static String TARGETJSONPATH = "$.target";
    final static String RELJSONPATH = "$.relType";

    public static void main(String[] args) throws Exception {


        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkScholexplorerMergeEntitiesJob.class.getResourceAsStream("/eu/dnetlib/dhp/graph/merge_entities_scholix_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkGraphImporterJob.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String targetPath = parser.get("targetPath");
        final String entity = parser.get("entity");


        FileSystem fs = FileSystem.get(sc.sc().hadoopConfiguration());
        List<Path> subFolder = Arrays.stream(fs.listStatus(new Path(inputPath))).filter(FileStatus::isDirectory).map(FileStatus::getPath).collect(Collectors.toList());
        List<JavaRDD<String>> inputRdd = new ArrayList<>();
        subFolder.forEach(p -> inputRdd.add(sc.textFile(p.toUri().getRawPath())));
        JavaRDD<String> union = sc.emptyRDD();
        for (JavaRDD<String> item : inputRdd) {
            union = union.union(item);
        }
        switch (entity) {
            case "dataset":
                union.mapToPair((PairFunction<String, String, DLIDataset>) f -> {
                    final String id = getJPathString(IDJSONPATH, f);
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    return new Tuple2<>(id, mapper.readValue(f, DLIDataset.class));
                }).reduceByKey((a, b) -> {
                    a.mergeFrom(b);
                    return a;
                }).map(item -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.writeValueAsString(item._2());
                }).saveAsTextFile(targetPath, GzipCodec.class);
                break;
            case "publication":
                union.mapToPair((PairFunction<String, String, DLIPublication>) f -> {
                    final String id = getJPathString(IDJSONPATH, f);
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    return new Tuple2<>(id, mapper.readValue(f, DLIPublication.class));
                }).reduceByKey((a, b) -> {
                    a.mergeFrom(b);
                    return a;
                }).map(item -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.writeValueAsString(item._2());
                }).saveAsTextFile(targetPath, GzipCodec.class);
                break;
            case "unknown":
                union.mapToPair((PairFunction<String, String, DLIUnknown>) f -> {
                    final String id = getJPathString(IDJSONPATH, f);
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    return new Tuple2<>(id, mapper.readValue(f, DLIUnknown.class));
                }).reduceByKey((a, b) -> {
                    a.mergeFrom(b);
                    return a;
                }).map(item -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.writeValueAsString(item._2());
                }).saveAsTextFile(targetPath, GzipCodec.class);
                break;
            case "relation":
                union.mapToPair((PairFunction<String, String, Relation>) f -> {
                    final String source = getJPathString(SOURCEJSONPATH, f);
                    final String target = getJPathString(TARGETJSONPATH, f);
                    final String reltype = getJPathString(RELJSONPATH, f);
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    return new Tuple2<>(DHPUtils.md5(String.format("%s::%s::%s", source, reltype, target)), mapper.readValue(f, Relation.class));
                }).reduceByKey((a, b) -> {
                    a.mergeOAFDataInfo(b);
                    return a;
                }).map(item -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.writeValueAsString(item._2());
                }).saveAsTextFile(targetPath, GzipCodec.class);
                break;
        }
    }

    public static String getJPathString(final String jsonPath, final String json) {
        try {
            Object o = JsonPath.read(json, jsonPath);
            if (o instanceof String)
                return (String) o;
            if (o instanceof JSONArray && ((JSONArray) o).size() > 0)
                return (String) ((JSONArray) o).get(0);
            return "";
        } catch (Exception e) {
            return "";
        }
    }
}
