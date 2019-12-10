package eu.dnetlib.dedup;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SparkCreateDedupRecord {

    public static void main(String[] args) throws Exception {
//        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateDedupRecord.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/dedup_parameters.json")));
//        parser.parseArgument(args);
//        final SparkSession spark = SparkSession
//                .builder()
//                .appName(SparkCreateDedupRecord.class.getSimpleName())
//                .master(parser.get("master"))
//                .getOrCreate();
//
//        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//        final String inputPath = parser.get("sourcePath");
//        final String entity = parser.get("entity");
//        final String targetPath = parser.get("targetPath");
////        final DedupConfig dedupConf = DedupConfig.load(IOUtils.toString(SparkCreateDedupRecord.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf2.json")));
//        final DedupConfig dedupConf = DedupConfig.load(parser.get("dedupConf"));
//
//        //<id, json_entity>
//        final JavaPairRDD<String, String> inputJsonEntities = sc.textFile(inputPath + "/" + entity)
//                .mapToPair((PairFunction<String,String,String>)it->
//                    new Tuple2<String,String>(MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), it),it)
//                );

//        //<source, target>: source is the dedup_id, target is the id of the mergedIn
//        JavaPairRDD<String,String> mergeRels = spark
//                .read().load(targetPath + "/" + entity+"_mergeRels").as(Encoders.bean(Relation.class))
//                .where("relClass=='merges'")
//                .javaRDD()
//                .mapToPair(
//                        (PairFunction<Relation, String,String>)r->
//                                new Tuple2<String,String>(r.getTarget(), r.getSource())
//                );
//
//        //<dedup_id, json_entity_merged>
//        final JavaPairRDD<String, String> p = mergeRels.join(inputJsonEntities).mapToPair((PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>) Tuple2::_2);

        StructType schema = Encoders.bean(Publication.class).schema();

        System.out.println(schema);
    }

}
