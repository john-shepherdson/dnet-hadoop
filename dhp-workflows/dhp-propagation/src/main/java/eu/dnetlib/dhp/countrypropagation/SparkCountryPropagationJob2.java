package eu.dnetlib.dhp.countrypropagation;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static eu.dnetlib.dhp.PropagationConstant.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SparkCountryPropagationJob2 {

    private static final Logger log = LoggerFactory.getLogger(SparkCountryPropagationJob2.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCountryPropagationJob2.class.getResourceAsStream("/eu/dnetlib/dhp/countrypropagation/input_countrypropagation_parameters.json")));
        parser.parseArgument(args);
        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkCountryPropagationJob2.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();


        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/countrytoresultfrominstitutionalrepositories";

       // createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        boolean writeUpdates = TRUE.equals(parser.get("writeUpdate"));
        boolean saveGraph = TRUE.equals(parser.get("saveGraph"));

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


        Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> dataset = spark.createDataset(sc.textFile(inputPath + "/dataset")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Dataset.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Dataset.class));

        Dataset<OtherResearchProduct> other = spark.createDataset(sc.textFile(inputPath + "/otherresearchproduct")
                        .map(item -> new ObjectMapper().readValue(item, OtherResearchProduct.class)).rdd(),
                Encoders.bean(OtherResearchProduct.class));

        Dataset<Software> software = spark.createDataset(sc.textFile(inputPath + "/software")
                        .map(item -> new ObjectMapper().readValue(item, Software.class)).rdd(),
                Encoders.bean(Software.class));

        Dataset<Publication> publication = spark.createDataset(sc.textFile(inputPath + "/publication")
                        .map(item -> new ObjectMapper().readValue(item, Publication.class)).rdd(),
                Encoders.bean(Publication.class));


        //todo broadcast


        software.createOrReplaceTempView("software");
        final JavaRDD<Row> toupdateresultsoftware = propagateOnResult(spark, "software");

        dataset.createOrReplaceTempView("dataset");
        final JavaRDD<Row> toupdateresultdataset = propagateOnResult(spark, "dataset");

        other.createOrReplaceTempView("other");
        final JavaRDD<Row> toupdateresultother = propagateOnResult(spark, "other");

        publication.createOrReplaceTempView("publication");
        final JavaRDD<Row> toupdateresultpublication = propagateOnResult(spark, "publication");

        if(writeUpdates){
            writeUpdates(toupdateresultsoftware, toupdateresultdataset, toupdateresultother, toupdateresultpublication, outputPath);
        }

        if(saveGraph){
            createUpdateForSoftwareDataset(toupdateresultsoftware, inputPath, spark)
                    .map(s -> new ObjectMapper().writeValueAsString(s))
                    .saveAsTextFile(outputPath + "/software");

            createUpdateForDatasetDataset(toupdateresultdataset,inputPath,spark)
                    .map(d -> new ObjectMapper().writeValueAsString(d))
                    .saveAsTextFile(outputPath + "/dataset");

            createUpdateForOtherDataset(toupdateresultother, inputPath, spark)
                    .map(o -> new ObjectMapper().writeValueAsString(o))
                    .saveAsTextFile(outputPath + "/otherresearchproduct");

            createUpdateForPublicationDataset(toupdateresultpublication, inputPath, spark)
                    .map(p -> new ObjectMapper().writeValueAsString(p))
                    .saveAsTextFile(outputPath + "/publication");
        }

    }

    private static void writeUpdates(JavaRDD<Row> software, JavaRDD<Row> dataset, JavaRDD<Row> other , JavaRDD<Row> publication, String outputPath){
        createUpdateForResultDatasetWrite(software, outputPath, "update_software");
        createUpdateForResultDatasetWrite(dataset, outputPath, "update_dataset");
        createUpdateForResultDatasetWrite(other, outputPath, "update_other");
        createUpdateForResultDatasetWrite(publication, outputPath, "update_publication");
    }

    private static JavaRDD<OtherResearchProduct> createUpdateForOtherDataset(JavaRDD<Row> toupdateresult, String inputPath, SparkSession spark) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return sc.textFile(inputPath + "/otherresearchproduct")
                .map(item -> new ObjectMapper().readValue(item, OtherResearchProduct.class))
                .mapToPair(s -> new Tuple2<>(s.getId(), s)).leftOuterJoin(getStringResultJavaPairRDD(toupdateresult))
                .map(c -> {
                    OtherResearchProduct oaf = c._2()._1();
                    List<Country> countryList = oaf.getCountry();
                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : countryList) {
                            countries.add(country.getClassid());
                        }
                        Result r = c._2()._2().get();
                        for (Country country : r.getCountry()) {
                            if (!countries.contains(country.getClassid())) {
                                countryList.add(country);
                            }
                        }
                        oaf.setCountry(countryList);
                    }
                    return oaf;
                });
    }

    private static JavaRDD<Publication> createUpdateForPublicationDataset(JavaRDD<Row> toupdateresult, String inputPath, SparkSession spark) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return sc.textFile(inputPath + "/publication")
                .map(item -> new ObjectMapper().readValue(item, Publication.class))
                .mapToPair(s -> new Tuple2<>(s.getId(), s)).leftOuterJoin(getStringResultJavaPairRDD(toupdateresult))
                .map(c -> {
                    Publication oaf = c._2()._1();
                    List<Country> countryList = oaf.getCountry();
                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : countryList) {
                            countries.add(country.getClassid());
                        }
                        Result r = c._2()._2().get();
                        for (Country country : r.getCountry()) {
                            if (!countries.contains(country.getClassid())) {
                                countryList.add(country);
                            }
                        }
                        oaf.setCountry(countryList);
                    }
                    return oaf;
                });
    }

    private static JavaRDD<Software> createUpdateForSoftwareDataset(JavaRDD<Row> toupdateresult, String inputPath, SparkSession spark) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return sc.textFile(inputPath + "/software")
                .map(item -> new ObjectMapper().readValue(item, Software.class))
                .mapToPair(s -> new Tuple2<>(s.getId(), s)).leftOuterJoin(getStringResultJavaPairRDD(toupdateresult))
                .map(c -> {
                    Software oaf = c._2()._1();
                    List<Country> countryList = oaf.getCountry();
                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : countryList) {
                            countries.add(country.getClassid());
                        }
                        Result r = c._2()._2().get();
                        for (Country country : r.getCountry()) {
                            if (!countries.contains(country.getClassid())) {
                                countryList.add(country);
                            }
                        }
                        oaf.setCountry(countryList);
                    }
                    return oaf;
                });
    }

    private static JavaRDD<eu.dnetlib.dhp.schema.oaf.Dataset> createUpdateForDatasetDataset(JavaRDD<Row> toupdateresult, String inputPath, SparkSession spark) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return sc.textFile(inputPath + "/dataset")
                .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Dataset.class))
                .mapToPair(d -> new Tuple2<>(d.getId(), d)).leftOuterJoin(getStringResultJavaPairRDD(toupdateresult))
                .map(c -> {
                    eu.dnetlib.dhp.schema.oaf.Dataset oaf = c._2()._1();
                    List<Country> countryList = oaf.getCountry();
                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : countryList) {
                            countries.add(country.getClassid());
                        }
                        Result r = c._2()._2().get();
                        for (Country country : r.getCountry()) {
                            if (!countries.contains(country.getClassid())) {
                                countryList.add(country);
                            }
                        }
                        oaf.setCountry(countryList);
                    }
                    return oaf;
                });
    }

    private static JavaRDD<Row> propagateOnResult(SparkSession spark, String result_type) {
        String query;
        query = "SELECT id, inst.collectedfrom.key cf , inst.hostedby.key hb " +
                "FROM ( SELECT id, instance " +
                "FROM " + result_type +
                " WHERE datainfo.deletedbyinference = false)  ds " +
                "LATERAL VIEW EXPLODE(instance) i AS inst";
        Dataset<Row> cfhb = spark.sql(query);
        cfhb.createOrReplaceTempView("cfhb");

        return countryPropagationAssoc(spark, "cfhb").toJavaRDD();

    }

    private static Dataset<Row> countryPropagationAssoc(SparkSession spark, String cfhbTable){
        String  query = "SELECT id, collect_set(country) country "+
                "FROM ( SELECT id, country " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON cf = ds     " +
                "UNION ALL " +
                "SELECT id , country     " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON hb = ds ) tmp " +
                "GROUP BY id";
        return spark.sql(query);
    }

    private static JavaPairRDD<String, Result> getStringResultJavaPairRDD(JavaRDD<Row> toupdateresult) {
        return toupdateresult.map(c -> {
            List<Country> countryList = new ArrayList<>();
            List<String> tmp = c.getList(1);
            for (String country : tmp) {
                countryList.add(getCountry(country));
            }
            Result r = new Result();
            r.setId(c.getString(0));
            r.setCountry(countryList);
            return r;
        }).mapToPair(r -> new Tuple2<>(r.getId(), r));
    }

    private static void createUpdateForResultDatasetWrite(JavaRDD<Row> toupdateresult, String outputPath, String type){
        toupdateresult.map(c -> {
            List<Country> countryList = new ArrayList<>();
            List<String> tmp = c.getList(1);
            for (String country : tmp) {
                countryList.add(getCountry(country));
            }
            Result r = new Result();
            r.setId(c.getString(0));
            r.setCountry(countryList);
            return r;

        }).map(r ->new ObjectMapper().writeValueAsString(r))
                .saveAsTextFile(outputPath+"/"+type);
    }



}