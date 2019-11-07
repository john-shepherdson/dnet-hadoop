package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkGraphImporterJob {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkGraphImporterJob.class.getResourceAsStream("/eu/dnetlib/dhp/graph/input_graph_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkGraphImporterJob.class.getSimpleName())
                .master(parser.get("master"))
                .config("hive.metastore.uris", parser.get("hive_metastore_uris"))
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String hiveDbName = parser.get("hive_db_name");

        spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", hiveDbName));

        // Read the input file and convert it into RDD of serializable object
        GraphMappingUtils.types.forEach((name, clazz) -> {
            final JavaRDD<Tuple2<String, String>> inputRDD = sc.sequenceFile(inputPath + "/" + name, Text.class, Text.class)
                    .map(item -> new Tuple2<>(item._1.toString(), item._2.toString()));

            spark.createDataset(inputRDD
                    .filter(s -> s._1().equals(clazz.getName()))
                    .map(Tuple2::_2)
                    .map(s -> new ObjectMapper().readValue(s, clazz))
                    .rdd(), Encoders.bean(clazz))
                    .limit(1000)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .saveAsTable(hiveDbName + "." + name);
        });

    }
}
