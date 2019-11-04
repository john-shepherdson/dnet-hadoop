package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Map;

public class SparkGraphImporterJob {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkGraphImporterJob.class.getResourceAsStream("/eu/dnetlib/dhp/graph/input_graph_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName("ImportGraph")
                .master(parser.get("master"))
                .getOrCreate();
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("input");
        final String outputPath = parser.get("outputDir");

        final String filter = parser.get("filter");

        // Read the input file and convert it into RDD of serializable object
        final JavaRDD<Tuple2<String, String>> inputRDD = sc.sequenceFile(inputPath, Text.class, Text.class)
                .map(item -> new Tuple2<>(item._1.toString(), item._2.toString()));

        final Map<String, Class> types = Maps.newHashMap();
        types.put("datasource", Datasource.class);
        types.put("organization", Organization.class);
        types.put("project", Project.class);
        types.put("dataset", Dataset.class);
        types.put("otherresearchproduct", OtherResearchProduct.class);
        types.put("software", Software.class);
        types.put("publication", Publication.class);
        types.put("relation", Relation.class);

        types.forEach((name, clazz) -> {
            if (StringUtils.isNotBlank(filter) || filter.toLowerCase().contains(name)) {
                spark.createDataset(inputRDD
                        .filter(s -> s._1().equals(clazz.getName()))
                        .map(Tuple2::_2)
                        .map(s -> new ObjectMapper().readValue(s, clazz))
                        .rdd(), Encoders.bean(clazz))
                        .write()
                        .save(outputPath + "/" + name);
            }
        });

    }
}
