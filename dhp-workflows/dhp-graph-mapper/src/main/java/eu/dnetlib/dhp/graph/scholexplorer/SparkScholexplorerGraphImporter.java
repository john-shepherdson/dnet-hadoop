package eu.dnetlib.dhp.graph.scholexplorer;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.graph.SparkGraphImporterJob;
import eu.dnetlib.dhp.graph.scholexplorer.parser.DatasetScholexplorerParser;
import eu.dnetlib.dhp.graph.scholexplorer.parser.PublicationScholexplorerParser;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkScholexplorerGraphImporter {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkScholexplorerGraphImporter.class.getResourceAsStream("/eu/dnetlib/dhp/graph/input_graph_scholix_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkGraphImporterJob.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");

        sc.sequenceFile(inputPath, IntWritable.class, Text.class).map(Tuple2::_2).map(Text::toString).repartition(500)
                .flatMap((FlatMapFunction<String, Oaf>) record -> {
                    switch (parser.get("entity")) {
                        case "dataset":
                            final DatasetScholexplorerParser d = new DatasetScholexplorerParser();
                            return d.parseObject(record).iterator();
                        case "publication":
                            final PublicationScholexplorerParser p = new PublicationScholexplorerParser();
                            return p.parseObject(record).iterator();
                        default:
                            throw new IllegalArgumentException("wrong values of entities");
                    }
                }).map(k -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(k);
        }).saveAsTextFile(parser.get("targetPath"), GzipCodec.class);
    }
}
