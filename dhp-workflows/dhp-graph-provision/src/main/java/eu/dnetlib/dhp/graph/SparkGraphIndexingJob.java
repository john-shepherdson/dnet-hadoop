package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkGraphIndexingJob {

    private final static String OUTPUT_BASE_PATH = "/tmp/openaire_provision";

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkGraphIndexingJob.class.getResourceAsStream("/eu/dnetlib/dhp/graph/input_graph_parameters.json")));
        parser.parseArgument(args);

        final SparkConf conf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        final SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .appName(SparkGraphIndexingJob.class.getSimpleName())
                .master(parser.get("master"))
                .enableHiveSupport()
                .getOrCreate();

        final String inputPath = parser.get("sourcePath");
        final String hiveDbName = parser.get("hive_db_name");

        final FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        if (fs.exists(new Path(OUTPUT_BASE_PATH))) {
            fs.delete(new Path(OUTPUT_BASE_PATH), true);
            fs.mkdirs(new Path(OUTPUT_BASE_PATH));
        }

        new GraphJoiner().join(spark, inputPath, hiveDbName, OUTPUT_BASE_PATH);
    }

}
