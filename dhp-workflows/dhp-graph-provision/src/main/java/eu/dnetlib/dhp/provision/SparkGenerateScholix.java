package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.Scholix;
import eu.dnetlib.dhp.provision.scholix.ScholixResource;
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SparkGenerateScholix {

    private static final String jsonIDPath = "$.id";
    private static final String sourceIDPath = "$.source";
    private static final String targetIDPath = "$.target";





    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkGenerateScholix.class.getResourceAsStream("/eu/dnetlib/dhp/provision/input_generate_summary_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkExtractRelationCount.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();


        final String graphPath = parser.get("graphPath");
        final String workingDirPath = parser.get("workingDirPath");

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


//        final JavaRDD<String> relationToExport = sc.textFile(graphPath + "/relation").filter(ProvisionUtil::isNotDeleted).repartition(4000);
        final JavaPairRDD<String,ScholixResource> scholixSummary =
                sc.textFile(workingDirPath + "/summary")
                        .flatMapToPair((PairFlatMapFunction<String, String, ScholixResource>) i -> {
                            final ObjectMapper mapper = new ObjectMapper();
                            final ScholixSummary summary = mapper.readValue(i, ScholixSummary.class);
                            ScholixResource tmp = ScholixResource.fromSummary(summary);
                            final List<Tuple2<String, ScholixResource>> result = new ArrayList<>();
                            for (int k = 0; k<10; k++)
                                result.add(new Tuple2<>(String.format("%s::%d", tmp.getDnetIdentifier(), k), tmp));
                            return result.iterator();
                        });
//        scholixSummary.join(
//                relationToExport
//                        .mapToPair((PairFunction<String, String, String>) i -> new Tuple2<>(DHPUtils.getJPathString(sourceIDPath, i), i)))
//                .map(Tuple2::_2)
//                .mapToPair(summaryRelation ->
//                        new Tuple2<>(
//                                DHPUtils.getJPathString(targetIDPath, summaryRelation._2()),
//                                Scholix.generateScholixWithSource(summaryRelation._1(), summaryRelation._2())))
//
//                .map(t-> t._2().setTarget(new ScholixResource().setDnetIdentifier(t._1())))
//                .map(s-> {
//                    ObjectMapper mapper = new ObjectMapper();
//                    return mapper.writeValueAsString(s);
//                })
//        .saveAsTextFile(workingDirPath + "/scholix", GzipCodec.class);

        sc.textFile(workingDirPath + "/scholix")
                .mapToPair(t -> {
                    ObjectMapper mapper = new ObjectMapper();
                    Scholix scholix = mapper.readValue(t, Scholix.class);
                    Random rand = new Random();
                    return new Tuple2<>(String.format("%s::%d",scholix.getTarget().getDnetIdentifier(), rand.nextInt(10)), scholix);
                })
                .join(scholixSummary)
                .map(t-> {
                    Scholix item = t._2()._1().setTarget(t._2()._2());
                    item.generateIdentifier();
                    return item;
                })
                .map(s-> new ObjectMapper().writeValueAsString(s)).saveAsTextFile(workingDirPath + "/scholix_index", GzipCodec.class);
    }



}
