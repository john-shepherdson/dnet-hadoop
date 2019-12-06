package eu.dnetlib.dedup;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
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
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SparkCreateDedupRecord {

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateDedupRecord.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/dedup_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkCreateDedupRecord.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String entity = parser.get("entity");
        final String targetPath = parser.get("targetPath");
        final DedupConfig dedupConf = DedupConfig.load(IOUtils.toString(SparkCreateDedupRecord.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf2.json")));

        final JavaPairRDD<String, String> inputJsonEntities = sc.textFile(inputPath + "/" + entity)
                .mapToPair((PairFunction<String,String,String>)it->
                    new Tuple2<String,String>(MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), it),it)
                );


//        JavaPairRDD<String,String> mergeRels = spark
//                .read().load(targetPath + "/" + entity+"_mergeRels").as(Encoders.bean(Relation.class))
//                .where("relClass=='merges'")
//                .javaRDD()
//                .mapToPair(
//                        (PairFunction<Relation, String,String>)r->
//                                new Tuple2<String,String>(r.getTarget(), r.getSource())
//                );
//
//
//        final JavaPairRDD<String, String> p = mergeRels.join(inputJsonEntities).mapToPair((PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>) Tuple2::_2);
//
//        Comparator<String> c = new Comparator<String>() {
//            @Override
//            public int compare(String s, String t1) {
//                return 0;
//            }
//        };
//        final JavaPairRDD<String, String> stringStringJavaPairRDD = p.repartitionAndSortWithinPartitions(p.partitioner().get(), c);


//        List<Foo> inputValues = Arrays.asList(
//                new Foo("k",5),
//                new Foo("a",1),
//                new Foo("a",30),
//                new Foo("a",18),
//                new Foo("a",22),
//                new Foo("b",22),
//                new Foo("c",5),
//                new Foo("a",5),
//                new Foo("s",1),
//                new Foo("h",4)
//        );
//
//
//        final JavaPairRDD<Foo, Foo> fooFighters = sc.parallelize(inputValues).mapToPair((PairFunction<Foo, Foo, Foo>) i -> new Tuple2<Foo, Foo>(i, i));
//
//
//        FooComparator c = new FooComparator();
//        final List<Tuple2<String, List<Foo>>> result =
//                fooFighters.repartitionAndSortWithinPartitions(new FooPartitioner(fooFighters.getNumPartitions()), c)
//                        .mapToPair((PairFunction<Tuple2<Foo, Foo>, String, Foo>) t-> new Tuple2<String,Foo>(t._1().getValue(), t._2()) )
//                        .groupByKey()
//                        .mapValues((Function<Iterable<Foo>, List<Foo>>) Lists::newArrayList)
//                        .collect();
//
//
//        System.out.println(result);

    }

}
