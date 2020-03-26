package eu.dnetlib.dhp.graph.scholexplorer;

import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkScholexplorerGenerateSimRel {

    final static String IDJSONPATH = "$.id";
    final static String OBJIDPATH = "$.originalObjIdentifier";



    public static void generateDataFrame(final SparkSession spark, final JavaSparkContext sc, final String inputPath, final String targetPath) {


        final JavaPairRDD<String, String> datasetSimRel = sc.textFile(inputPath+"/dataset/*")
                .mapToPair((PairFunction<String, String, String>) k ->
                        new Tuple2<>(DHPUtils.getJPathString(IDJSONPATH, k),DHPUtils.getJPathString(OBJIDPATH, k)))
                .filter(t ->
                        !StringUtils.substringAfter(t._1(), "|")
                                .equalsIgnoreCase(StringUtils.substringAfter(t._2(), "::")))
                .distinct();

        final JavaPairRDD<String, String> publicationSimRel = sc.textFile(inputPath+"/publication/*")
                .mapToPair((PairFunction<String, String, String>) k ->
                        new Tuple2<>(DHPUtils.getJPathString(IDJSONPATH, k),DHPUtils.getJPathString(OBJIDPATH, k)))
                .filter(t ->
                        !StringUtils.substringAfter(t._1(), "|")
                                .equalsIgnoreCase(StringUtils.substringAfter(t._2(), "::")))
                .distinct();

        JavaRDD<Relation> simRel = datasetSimRel.union(publicationSimRel).map(s -> {
                    final Relation r = new Relation();
                    r.setSource(s._1());
                    r.setTarget(s._2());
                    r.setRelType("similar");
                    return r;
                }
        );
        spark.createDataset(simRel.rdd(), Encoders.bean(Relation.class)).distinct().write()
                .mode(SaveMode.Overwrite).save(targetPath+"/pid_simRel");
    }
}
