package eu.dnetlib.dhp.graph.scholexplorer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.graph.SparkGraphImporterJob;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.scholexplorer.DLIDataset;
import eu.dnetlib.dhp.schema.scholexplorer.DLIPublication;
import eu.dnetlib.dhp.schema.scholexplorer.DLIUnknown;
import eu.dnetlib.dhp.utils.DHPUtils;
import net.minidev.json.JSONArray;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.collection.JavaConverters;
import sun.rmi.log.ReliableLog;

import javax.xml.crypto.Data;
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
                .config(new SparkConf()
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
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

                SparkScholexplorerGenerateSimRel.generateDataFrame(spark, sc, inputPath.replace("/relation",""),targetPath.replace("/relation","") );
                RDD<Relation> rdd = union.mapToPair((PairFunction<String, String, Relation>) f -> {
                    final String source = getJPathString(SOURCEJSONPATH, f);
                    final String target = getJPathString(TARGETJSONPATH, f);
                    final String reltype = getJPathString(RELJSONPATH, f);
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    return new Tuple2<>(DHPUtils.md5(String.format("%s::%s::%s", source.toLowerCase(), reltype.toLowerCase(), target.toLowerCase())), mapper.readValue(f, Relation.class));
                }).reduceByKey((a, b) -> {
                    a.mergeFrom(b);
                    return a;
                }).map(Tuple2::_2).rdd();

                spark.createDataset(rdd, Encoders.bean(Relation.class)).write().mode(SaveMode.Overwrite).save(targetPath);
                Dataset<Relation> rel_ds =spark.read().load(targetPath).as(Encoders.bean(Relation.class));

                System.out.println("LOADING PATH :"+targetPath.replace("/relation","")+"/pid_simRel");
                Dataset<Relation>sim_ds  =spark.read().load(targetPath.replace("/relation","")+"/pid_simRel").as(Encoders.bean(Relation.class));

                TargetFunction tf = new TargetFunction();

                Dataset<Relation> ids = sim_ds.map(tf, Encoders.bean(Relation.class));


                final Dataset<Relation> firstJoin = rel_ds
                        .joinWith(ids, ids.col("target")
                                .equalTo(rel_ds.col("source")), "left_outer")
                        .map((MapFunction<Tuple2<Relation, Relation>, Relation>) s ->
                                {
                                    if (s._2() != null) {
                                        s._1().setSource(s._2().getSource());
                                    }
                                    return s._1();
                                }
                                , Encoders.bean(Relation.class));


                Dataset<Relation> secondJoin = firstJoin.joinWith(ids, ids.col("target").equalTo(firstJoin.col("target")),"left_outer")
                        .map((MapFunction<Tuple2<Relation, Relation>, Relation>) s ->
                                {
                                    if (s._2() != null) {
                                        s._1().setTarget(s._2().getSource());
                                    }
                                    return s._1();
                                }
                                , Encoders.bean(Relation.class));
                    secondJoin.write().mode(SaveMode.Overwrite).save(targetPath+"_fixed");
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
