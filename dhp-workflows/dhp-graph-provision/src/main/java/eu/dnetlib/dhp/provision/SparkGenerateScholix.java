package eu.dnetlib.dhp.provision;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.*;
import eu.dnetlib.dhp.provision.scholix.summary.*;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.*;

import  static org.apache.spark.sql.functions.col;

import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SparkGenerateScholix {


    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkGenerateScholix.class.getResourceAsStream("/eu/dnetlib/dhp/provision/input_generate_summary_parameters.json")));
        parser.parseArgument(args);


        SparkConf conf = new SparkConf();
        conf.set("spark.sql.shuffle.partitions","4000");
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.registerKryoClasses(new Class[]{
//                ScholixSummary.class,
//                CollectedFromType.class,
//                SchemeValue.class,
//                TypedIdentifier.class,
//                Typology.class,
//                Relation.class,
//                Scholix.class,
//                ScholixCollectedFrom.class,
//                ScholixEntityId.class,
//                ScholixIdentifier.class,
//                ScholixRelationship.class,
//                ScholixResource.class
//        });


        final SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .appName(SparkExtractRelationCount.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();


        final String graphPath = parser.get("graphPath");
        final String workingDirPath = parser.get("workingDirPath");

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        final Dataset<ScholixSummary> scholixSummary = spark.read().load(workingDirPath + "/summary").as(Encoders.bean(ScholixSummary.class));
        final Dataset<Relation>  rels = spark.read().load(graphPath + "/relation").as(Encoders.bean(Relation.class));


        Dataset<Scholix> firstJoin = scholixSummary.joinWith(rels, scholixSummary.col("id").equalTo(rels.col("source")))
                .map((MapFunction<Tuple2<ScholixSummary, Relation>, Scholix>) f -> Scholix.generateScholixWithSource(f._1(), f._2()), Encoders.bean(Scholix.class));

        firstJoin.write().mode(SaveMode.Overwrite).save(workingDirPath+"/scholix_1");
        firstJoin = spark.read().load(workingDirPath+"/scholix_1").as(Encoders.bean(Scholix.class));



        Dataset<Scholix> scholix_final = spark.read().load(workingDirPath+"/scholix_1").as(Encoders.bean(Scholix.class));

        Dataset<ScholixResource> target = spark.read().load(workingDirPath+"/scholix_target").as(Encoders.bean(ScholixResource.class));

        scholix_final.joinWith(target, scholix_final.col("identifier").equalTo(target.col("dnetIdentifier")), "inner")
                .map((MapFunction<Tuple2<Scholix, ScholixResource>, Scholix>) f -> {
                    final Scholix scholix = f._1();
                    final ScholixResource scholixTarget = f._2();
                    scholix.setTarget(scholixTarget);
                    scholix.generateIdentifier();
                    scholix.generatelinkPublisher();
                    return scholix;
                }, Encoders.bean(Scholix.class)).repartition(5000).write().mode(SaveMode.Overwrite).save(workingDirPath+"/scholix_index");
    }
}
