package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkXmlRecordBuilderJob {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkXmlRecordBuilderJob.class.getResourceAsStream("/eu/dnetlib/dhp/graph/input_graph_parameters.json")));
        parser.parseArgument(args);

        final SparkConf conf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        final SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .appName(SparkXmlRecordBuilderJob.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();

        final String inputPath = parser.get("sourcePath");
        final String outputPath = parser.get("outputPath");

        final FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
            fs.mkdirs(new Path(outputPath));
        }

        new GraphJoiner(spark, inputPath, outputPath)
                .adjacencyLists();
    }

}
