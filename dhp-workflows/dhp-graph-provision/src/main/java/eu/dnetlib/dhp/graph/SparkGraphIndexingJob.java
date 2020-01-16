package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

public class SparkGraphIndexingJob {

    private final static String ENTITY_NODES_PATH = "/tmp/entity_node";

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkGraphIndexingJob.class.getResourceAsStream("/eu/dnetlib/dhp/graph/input_graph_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkGraphIndexingJob.class.getSimpleName())
                .master(parser.get("master"))
                .config("hive.metastore.uris", parser.get("hive_metastore_uris"))
                .enableHiveSupport()
                .getOrCreate();

        final String inputPath = parser.get("sourcePath");
        final String hiveDbName = parser.get("hive_db_name");

        final FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        if (fs.exists(new Path(ENTITY_NODES_PATH))) {
            fs.delete(new Path(ENTITY_NODES_PATH), true);
        }

        new GraphJoiner().join(spark, inputPath, hiveDbName, ENTITY_NODES_PATH);
    }

}
