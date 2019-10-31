package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.util.ProtoConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

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

        final JavaRDD<Oaf> oafRdd = inputRDD.filter(s -> !StringUtils.isBlank(s._2()) && !s._1().contains("@update")).map(Tuple2::_2).map(ProtoConverter::convert);

        final Encoder<Organization> organizationEncoder = Encoders.bean(Organization.class);
        final Encoder<Project> projectEncoder = Encoders.bean(Project.class);
        final Encoder<Datasource> datasourceEncoder = Encoders.bean(Datasource.class);

        final Encoder<eu.dnetlib.dhp.schema.oaf.Dataset> datasetEncoder = Encoders.bean(eu.dnetlib.dhp.schema.oaf.Dataset.class);
        final Encoder<Publication> publicationEncoder = Encoders.bean(Publication.class);
        final Encoder<Software> softwareEncoder = Encoders.bean(Software.class);
        final Encoder<OtherResearchProducts> otherResearchProductsEncoder = Encoders.bean(OtherResearchProducts.class);

        final Encoder<Relation> relationEncoder = Encoders.bean(Relation.class);

        if (StringUtils.isBlank(filter)|| filter.toLowerCase().contains("organization"))
            spark.createDataset(oafRdd.filter(s -> s instanceof Organization).map(s -> (Organization) s).rdd(), organizationEncoder).write().save(outputPath + "/organizations");
        if (StringUtils.isBlank(filter)|| filter.toLowerCase().contains("project"))
        spark.createDataset(oafRdd.filter(s -> s instanceof Project).map(s -> (Project) s).rdd(), projectEncoder).write().save(outputPath + "/projects");
        if (StringUtils.isBlank(filter)|| filter.toLowerCase().contains("datasource"))
            spark.createDataset(oafRdd.filter(s -> s instanceof Datasource).map(s -> (Datasource) s).rdd(), datasourceEncoder).write().save(outputPath + "/datasources");
        if (StringUtils.isBlank(filter)|| filter.toLowerCase().contains("dataset"))
            spark.createDataset(oafRdd.filter(s -> s instanceof eu.dnetlib.dhp.schema.oaf.Dataset).map(s -> (eu.dnetlib.dhp.schema.oaf.Dataset) s).rdd(), datasetEncoder).write().save(outputPath + "/datasets");

        if (StringUtils.isBlank(filter)|| filter.toLowerCase().contains("publication"))
            spark.createDataset(oafRdd.filter(s -> s instanceof Publication).map(s -> (Publication) s).rdd(), publicationEncoder).write().save(outputPath + "/publications");
        if (StringUtils.isBlank(filter)|| filter.toLowerCase().contains("software"))
            spark.createDataset(oafRdd.filter(s -> s instanceof Software).map(s -> (Software) s).rdd(), softwareEncoder).write().save(outputPath + "/software");
        if (StringUtils.isBlank(filter)|| filter.toLowerCase().contains("otherresearchproduct"))
            spark.createDataset(oafRdd.filter(s -> s instanceof OtherResearchProducts).map(s -> (OtherResearchProducts) s).rdd(), otherResearchProductsEncoder).write().save(outputPath + "/otherResearchProducts");
        if (StringUtils.isBlank(filter)|| filter.toLowerCase().contains("relation"))
            spark.createDataset(oafRdd.filter(s -> s instanceof Relation).map(s -> (Relation) s).rdd(), relationEncoder).write().save(outputPath + "/relations");
    }
}
