package eu.dnetlib.dhp.graph.scholexplorer;

import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.SparkGraphImporterJob;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import net.minidev.json.JSONArray;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class SparkExtractEntitiesJob {
    final static String IDJSONPATH = "$.id";
    final static String SOURCEJSONPATH = "$.source";
    final static String TARGETJSONPATH = "$.target";


    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkExtractEntitiesJob.class.getResourceAsStream(
                                "/eu/dnetlib/dhp/graph/input_extract_entities_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkExtractEntitiesJob.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String targetPath = parser.get("targetPath");
        final String tdir =parser.get("targetDir");
        final JavaRDD<String> inputRDD = sc.textFile(inputPath);

        List<String> entities = Arrays.stream(parser.get("entities").split(",")).map(String::trim).collect(Collectors.toList());
        if (entities.stream().anyMatch("dataset"::equalsIgnoreCase)) {
            //Extract Dataset
            inputRDD.filter(SparkExtractEntitiesJob::isDataset).saveAsTextFile(targetPath + "/dataset/"+tdir, GzipCodec.class);
        }
        if (entities.stream().anyMatch("unknown"::equalsIgnoreCase)) {
            //Extract Unknown
            inputRDD.filter(SparkExtractEntitiesJob::isUnknown).saveAsTextFile(targetPath + "/unknown/"+tdir, GzipCodec.class);
        }

        if (entities.stream().anyMatch("relation"::equalsIgnoreCase)) {
            //Extract Relation
            inputRDD.filter(SparkExtractEntitiesJob::isRelation).saveAsTextFile(targetPath + "/relation/"+tdir, GzipCodec.class);
        }
        if (entities.stream().anyMatch("publication"::equalsIgnoreCase)) {
            //Extract Relation
            inputRDD.filter(SparkExtractEntitiesJob::isPublication).saveAsTextFile(targetPath + "/publication/"+tdir, GzipCodec.class);
        }
    }


    public static boolean isDataset(final String json) {
        final String id = getJPathString(IDJSONPATH, json);
        if (StringUtils.isBlank(id)) return false;
        return id.startsWith("60|");
    }


    public static boolean isPublication(final String json) {
        final String id = getJPathString(IDJSONPATH, json);
        if (StringUtils.isBlank(id)) return false;
        return id.startsWith("50|");
    }

    public static boolean isUnknown(final String json) {
        final String id = getJPathString(IDJSONPATH, json);
        if (StringUtils.isBlank(id)) return false;
        return id.startsWith("70|");
    }

    public static boolean isRelation(final String json) {
        final String source = getJPathString(SOURCEJSONPATH, json);
        final String target = getJPathString(TARGETJSONPATH, json);
        return StringUtils.isNotBlank(source) && StringUtils.isNotBlank(target);
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
