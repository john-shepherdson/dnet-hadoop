package eu.dnetlib.dhp.countrypropagation;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

import java.util.*;

import static eu.dnetlib.dhp.PropagationConstant.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SparkCountryPropagationJob2 {

    private static final Logger log = LoggerFactory.getLogger(SparkCountryPropagationJob2.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils.toString(SparkCountryPropagationJob2.class
                .getResourceAsStream("/eu/dnetlib/dhp/countrypropagation/input_countrypropagation_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                jsonConfiguration);

        parser.parseArgument(args);

        String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = "/tmp/provision/propagation/countrytoresultfrominstitutionalrepositories";
        final String datasourcecountrypath = outputPath + "/prepared_datasource_country";
        final String resultClassName = parser.get("resultClazz");

        final String resultType = resultClassName.substring(resultClassName.lastIndexOf(".")+1);

        Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkCountryPropagationJob2.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        final boolean writeUpdates = TRUE.equals(parser.get("writeUpdate"));
        final boolean saveGraph = TRUE.equals(parser.get("saveGraph"));

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        //Load parque file with preprocessed association datasource - country
        Dataset<DatasourceCountry> datasourcecountryassoc = readAssocDatasourceCountry(spark, datasourcecountrypath);
        //broadcasting the result of the preparation step
        Broadcast<Dataset<DatasourceCountry>> broadcast_datasourcecountryassoc = sc.broadcast(datasourcecountryassoc);

        Dataset<Row> potentialUpdates = getPotentialResultToUpdate(spark, inputPath, resultClazz, broadcast_datasourcecountryassoc);

        if(writeUpdates){
            writeUpdates(potentialUpdates.toJavaRDD(), outputPath + "/update_" + resultType);
        }

        if(saveGraph){
            updateResultTable(spark, potentialUpdates, inputPath, resultClazz, outputPath + "/" + resultType);

        }

    }

    private static <R extends Result> void updateResultTable(SparkSession spark, Dataset<Row> potentialUpdates,
                                                             String inputPath,
                                                             Class<R> resultClazz,
                                                             String outputPath) {

        Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);

        Dataset<Tuple2<String, R>> result_pair = result
                .map(r -> new Tuple2<>(r.getId(), r),
                Encoders.tuple(Encoders.STRING(), Encoders.bean(resultClazz)));

        Dataset<Tuple2<String, List>> potential_update_pair = potentialUpdates.map(pu -> new Tuple2<>(pu.getString(0), pu.getList(1)),
                Encoders.tuple(Encoders.STRING(), Encoders.bean(List.class)));

        Dataset<R> new_table = result_pair
                .joinWith(potential_update_pair, result_pair.col("_1").equalTo(potential_update_pair.col("_1")), "left")
                .map((MapFunction<Tuple2<Tuple2<String, R>, Tuple2<String, List>>, R>) value -> {
                    R r = value._1()._2();
                    Optional<List<Object>> potentialNewCountries = Optional.ofNullable(value._2()).map(Tuple2::_2);
                    if (potentialNewCountries != null) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : r.getCountry()) {
                            countries.add(country.getClassid());
                        }

                        for (Object country : potentialNewCountries.get()) {
                            if (!countries.contains(country)) {
                                r.getCountry().add(getCountry((String) country));
                            }
                        }
                    }
                    return r;

                }, Encoders.bean(resultClazz));


            log.info("Saving graph table to path: {}", outputPath);
            result
                    .toJSON()
                    .write()
                    .option("compression", "gzip")
                    .text(outputPath);
        }
        


    private static <R extends Result> Dataset<Row> getPotentialResultToUpdate(SparkSession spark, String inputPath,
                                                                    Class<R> resultClazz,
                                                                    Broadcast<Dataset<DatasourceCountry>> broadcast_datasourcecountryassoc) {

        Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);
        result.createOrReplaceTempView("result");
        createCfHbforresult(spark);
        return countryPropagationAssoc(spark, broadcast_datasourcecountryassoc);
    }


    private static void createCfHbforresult(SparkSession spark) {
        String query;
        query = "SELECT id, inst.collectedfrom.key cf , inst.hostedby.key hb " +
                "FROM ( SELECT id, instance " +
                "FROM result " +
                " WHERE datainfo.deletedbyinference = false)  ds " +
                "LATERAL VIEW EXPLODE(instance) i AS inst";
        Dataset<Row> cfhb = spark.sql(query);
        cfhb.createOrReplaceTempView("cfhb");
    }


    private static Dataset<Row> countryPropagationAssoc(SparkSession spark,
                                                        Broadcast<Dataset<DatasourceCountry>> broadcast_datasourcecountryassoc){
        Dataset<DatasourceCountry> datasource_country = broadcast_datasourcecountryassoc.value();
        datasource_country.createOrReplaceTempView("datasource_country");

        String  query = "SELECT id, collect_set(country) country "+
                "FROM ( SELECT id, country " +
                "FROM rels " +
                "JOIN cfhb "  +
                " ON cf = ds     " +
                "UNION ALL " +
                "SELECT id , country     " +
                "FROM rels " +
                "JOIN cfhb " +
                " ON hb = ds ) tmp " +
                "GROUP BY id";
        return spark.sql(query);
    }

    private static <R extends Result> Dataset<R> readPathEntity(SparkSession spark, String inputEntityPath, Class<R> resultClazz) {

        log.info("Reading Graph table from: {}", inputEntityPath);
        return spark
                .read()
                .textFile(inputEntityPath)
                .map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, resultClazz), Encoders.bean(resultClazz));
    }

    private static Dataset<DatasourceCountry> readAssocDatasourceCountry(SparkSession spark, String relationPath) {
         return spark.read()
                .load(relationPath)
                .as(Encoders.bean(DatasourceCountry.class));
    }

    private static void writeUpdates(JavaRDD<Row> potentialUpdates, String outputPath){
        potentialUpdates.map(u -> OBJECT_MAPPER.writeValueAsString(u))
                .saveAsTextFile(outputPath);
    }



}