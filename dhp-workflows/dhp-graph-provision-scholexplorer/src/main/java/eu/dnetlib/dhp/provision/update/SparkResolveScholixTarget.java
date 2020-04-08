package eu.dnetlib.dhp.provision.update;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.Scholix;
import eu.dnetlib.dhp.provision.scholix.ScholixIdentifier;
import eu.dnetlib.dhp.provision.scholix.ScholixResource;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Collections;

public class SparkResolveScholixTarget {

    public static void main(String[] args) throws  Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResolveScholixTarget.class.getResourceAsStream("/eu/dnetlib/dhp/provision/input_resolve_scholix_parameters.json")));
        parser.parseArgument(args);

        final SparkConf conf = new SparkConf();

        final String master = parser.get("master");
        final String sourcePath = parser.get("sourcePath");
        final String workingDirPath= parser.get("workingDirPath");
        final String indexHost= parser.get("indexHost");


        try (SparkSession spark = getSession(conf, master)){

            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


            spark.createDataset(sc.sequenceFile(sourcePath, IntWritable.class,Text.class)
                    .map(Tuple2::_2)
                    .map(s-> new ObjectMapper().readValue(s.toString(), Scholix.class)).rdd(), Encoders.bean(Scholix.class))
                    .write().save(workingDirPath+"/stepA");



            Dataset<Scholix> s1 = spark.read().load(workingDirPath+"/stepA").as(Encoders.bean(Scholix.class));

            s1.where(s1.col("target.dnetIdentifier").isNull()).select(s1.col("target.identifier")).distinct()
                    .map((MapFunction<Row, ScholixResource>) f-> {
                        final String pid = ((Row) f.getList(0).get(0)).getString(0);
                        ScholixResource publication  = new CrossrefClient(indexHost).getResourceByDOI(pid);
                        if (publication != null) {
                            return  publication;
                        }
                        ScholixResource dataset = new DataciteClient(indexHost).getDatasetByDOI(pid);
                        if (dataset!= null) {
                            return dataset;
                        }
                        ScholixResource r = new ScholixResource();
                        r.setIdentifier(Collections.singletonList(new ScholixIdentifier(pid, "doi")));
                        r.setObjectType("unknown");
                        r.setDnetIdentifier("70|"+DHPUtils.md5(String.format("%s::doi", pid.toLowerCase().trim())));

                        return r;
                    }, Encoders.bean(ScholixResource.class)).write().mode(SaveMode.Overwrite).save(workingDirPath+"/stepB");



        }
    }

    private static SparkSession getSession(SparkConf conf, String master) {
        return SparkSession
                .builder()
                .config(conf)
                .appName(SparkResolveScholixTarget.class.getSimpleName())
                .master(master)
                .getOrCreate();
    }

}
