package eu.dnetlib.dhp.graph;


import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Publication;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import javax.xml.crypto.Data;

public class SparkGraphImporterJob {


    public static void main(String[] args) throws Exception{

        //TODO add argument parser
//        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkGraphImporterJob.class.getResourceAsStream("/eu/dnetlib/dhp/graph/graph_importer_parameters.json")));
//        parser.parseArgument(args);

        final SparkSession spark = SparkSession
                .builder()
                .appName("ImportGraph")
                //TODO replace with: master(parser.get("master"))
                .master("local[16]")
                .getOrCreate();


        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        final String path = "file:///Users/miconis/Downloads/part-m-02236";
        final JavaRDD<Tuple2<String, String>> inputRDD = sc.sequenceFile(path, Text.class, Text.class)
                .map(item -> new Tuple2<>(item._1.toString(), item._2.toString()));

        final JavaRDD<Datasource> datasources = inputRDD
                .filter(s -> s._1().split("@")[2].equalsIgnoreCase("body"))
                .map(Tuple2::_2)
                .map(ProtoConverter::convert)
                .filter(s-> s instanceof Datasource)
                .map(s->(Datasource)s);
        final Encoder<Datasource> encoder = Encoders.bean(Datasource.class);
        final Dataset<Datasource> mdstore = spark.createDataset(datasources.rdd(), encoder);

        System.out.println(mdstore.count());

//
//
//                .filter(s -> s instanceof Publication)
//                .count();




    }
}
