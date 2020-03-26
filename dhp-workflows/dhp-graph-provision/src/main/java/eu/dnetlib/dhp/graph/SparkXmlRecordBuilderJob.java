package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.graph.utils.ContextMapper;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkXmlRecordBuilderJob {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkXmlRecordBuilderJob.class.getResourceAsStream("/eu/dnetlib/dhp/graph/input_params_build_adjacency_lists.json")));
        parser.parseArgument(args);

        final String master = parser.get("master");
        final SparkConf conf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        try(SparkSession spark = getSession(conf, master)) {

            final String inputPath = parser.get("sourcePath");
            final String outputPath = parser.get("outputPath");
            final String isLookupUrl = parser.get("isLookupUrl");
            final String otherDsTypeId = parser.get("otherDsTypeId");

            final FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            if (fs.exists(new Path(outputPath))) {
                fs.delete(new Path(outputPath), true);
                fs.mkdirs(new Path(outputPath));
            }

            new GraphJoiner(spark, ContextMapper.fromIS(isLookupUrl), otherDsTypeId, inputPath, outputPath)
                    .adjacencyLists();
                    //.asXML();
        }
    }

    private static SparkSession getSession(SparkConf conf, String master) {
        return SparkSession
                .builder()
                .config(conf)
                .appName(SparkXmlRecordBuilderJob.class.getSimpleName())
                .master(master)
                .getOrCreate();
    }

}
