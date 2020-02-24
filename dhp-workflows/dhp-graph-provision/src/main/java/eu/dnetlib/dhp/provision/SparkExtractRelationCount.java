package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.utils.DHPUtils;
import net.minidev.json.JSONArray;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;


/**
 * SparkExtractRelationCount is a spark job that takes in input relation RDD
 * and retrieve for each item in relation which are the number of
 * - Related Dataset
 * - Related Publication
 * - Related Unknown
 */
public class SparkExtractRelationCount {





    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkExtractRelationCount.class.getResourceAsStream("/eu/dnetlib/dhp/provision/input_related_entities_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkExtractRelationCount.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();


        final String workingDirPath = parser.get("workingDirPath");

        final String relationPath = parser.get("relationPath");

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        sc.textFile(relationPath)
                // We start to Filter the relation not deleted by Inference
                .filter(ProvisionUtil::isNotDeleted)
                // Then we create a PairRDD<String, RelatedItem>
                .mapToPair((PairFunction<String, String, RelatedItemInfo>) f
                        -> new Tuple2<>(DHPUtils.getJPathString(ProvisionUtil.SOURCEJSONPATH, f), ProvisionUtil.getItemType(f, ProvisionUtil.TARGETJSONPATH)))
                //We reduce and sum the number of Relations
                .reduceByKey((Function2<RelatedItemInfo, RelatedItemInfo, RelatedItemInfo>) (v1, v2) -> {
                    if (v1 == null && v2 == null)
                        return new RelatedItemInfo();
                    return v1 != null ? v1.add(v2) : v2;
                })
                //Set the source Id in RelatedItem object
                .map(k -> k._2().setId(k._1()))
                // Convert to JSON and save as TextFile
                .map(k -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.writeValueAsString(k);
                }).saveAsTextFile(workingDirPath + "/relatedItemCount", GzipCodec.class);
    }







}
