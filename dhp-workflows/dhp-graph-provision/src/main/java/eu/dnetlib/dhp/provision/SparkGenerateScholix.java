package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.Scholix;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

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


        final JavaRDD<String> relationToExport = sc.textFile(graphPath + "/relation").filter(ProvisionUtil::isNotDeleted).repartition(4000);
        final JavaPairRDD<String,String> scholixSummary = sc.textFile(workingDirPath + "/summary").mapToPair((PairFunction<String, String, String>) i -> new Tuple2<>(DHPUtils.getJPathString(jsonIDPath, i), i));
        scholixSummary.join(
                relationToExport
                        .mapToPair((PairFunction<String, String, String>) i -> new Tuple2<>(DHPUtils.getJPathString(sourceIDPath, i), i)))
                .map(Tuple2::_2)
                .mapToPair(summaryRelation ->
                        new Tuple2<>(
                                DHPUtils.getJPathString(targetIDPath,summaryRelation._2()),
                                Scholix.generateScholixWithSource(summaryRelation._1(), summaryRelation._2())))
                .join(scholixSummary)
                .map(Tuple2::_2)
                .map(i -> i._1().addTarget(i._2()))
                .map(s-> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.writeValueAsString(s);
                })
        .saveAsTextFile(workingDirPath + "/scholix", GzipCodec.class);


        ;


    }



}
