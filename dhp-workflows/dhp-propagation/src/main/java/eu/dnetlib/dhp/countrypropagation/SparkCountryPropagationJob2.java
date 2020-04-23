package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import java.util.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkCountryPropagationJob2 {

    private static final Logger log = LoggerFactory.getLogger(SparkCountryPropagationJob2.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration =
                IOUtils.toString(
                        SparkCountryPropagationJob2.class.getResourceAsStream(
                                "/eu/dnetlib/dhp/countrypropagation/input_countrypropagation_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        final String datasourcecountrypath = parser.get("preparedInfoPath");
        log.info("preparedInfoPath: {}", datasourcecountrypath);

        final String resultClassName = parser.get("resultTableName");
        log.info("resultTableName: {}", resultClassName);

        final String resultType =
                resultClassName.substring(resultClassName.lastIndexOf(".") + 1).toLowerCase();
        log.info("resultType: {}", resultType);

        final Boolean writeUpdates =
                Optional.ofNullable(parser.get("writeUpdate"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.TRUE);
        log.info("writeUpdate: {}", writeUpdates);

        final Boolean saveGraph =
                Optional.ofNullable(parser.get("saveGraph"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.TRUE);
        log.info("saveGraph: {}", saveGraph);

        Class<? extends Result> resultClazz =
                (Class<? extends Result>) Class.forName(resultClassName);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        runWithSparkHiveSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    // createOutputDirs(outputPath,
                    // FileSystem.get(spark.sparkContext().hadoopConfiguration()));
                    removeOutputDir(spark, outputPath);
                    execPropagation(
                            spark,
                            datasourcecountrypath,
                            inputPath,
                            outputPath,
                            resultClazz,
                            resultType,
                            writeUpdates,
                            saveGraph);
                });
    }

    private static <R extends Result> void execPropagation(
            SparkSession spark,
            String datasourcecountrypath,
            String inputPath,
            String outputPath,
            Class<R> resultClazz,
            String resultType,
            boolean writeUpdates,
            boolean saveGraph) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Load parque file with preprocessed association datasource - country
        Dataset<DatasourceCountry> datasourcecountryassoc =
                readAssocDatasourceCountry(spark, datasourcecountrypath);
        // broadcasting the result of the preparation step
        Broadcast<Dataset<DatasourceCountry>> broadcast_datasourcecountryassoc =
                sc.broadcast(datasourcecountryassoc);

        Dataset<ResultCountrySet> potentialUpdates =
                getPotentialResultToUpdate(
                                spark, inputPath, resultClazz, broadcast_datasourcecountryassoc)
                        .as(Encoders.bean(ResultCountrySet.class));

        if (writeUpdates) {
            writeUpdates(potentialUpdates, outputPath + "/update_" + resultType);
        }

        if (saveGraph) {
            updateResultTable(spark, potentialUpdates, inputPath, resultClazz, outputPath);
        }
    }

    private static <R extends Result> void updateResultTable(
            SparkSession spark,
            Dataset<ResultCountrySet> potentialUpdates,
            String inputPath,
            Class<R> resultClazz,
            String outputPath) {

        log.info("Reading Graph table from: {}", inputPath);
        Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);

        Dataset<Tuple2<String, R>> result_pair =
                result.map(
                        r -> new Tuple2<>(r.getId(), r),
                        Encoders.tuple(Encoders.STRING(), Encoders.bean(resultClazz)));

        //        Dataset<Tuple2<String, ResultCountrySet>> potential_update_pair =
        // potentialUpdates.map(pu -> new Tuple2<>(pu.getResultId(),
        //                        pu),
        //                Encoders.tuple(Encoders.STRING(), Encoders.bean(ResultCountrySet.class)));

        Dataset<R> new_table =
                result_pair
                        .joinWith(
                                potentialUpdates,
                                result_pair.col("_1").equalTo(potentialUpdates.col("resultId")),
                                "left_outer")
                        .map(
                                (MapFunction<Tuple2<Tuple2<String, R>, ResultCountrySet>, R>)
                                        value -> {
                                            R r = value._1()._2();
                                            Optional<ResultCountrySet> potentialNewCountries =
                                                    Optional.ofNullable(value._2());
                                            if (potentialNewCountries.isPresent()) {
                                                HashSet<String> countries = new HashSet<>();
                                                for (Qualifier country : r.getCountry()) {
                                                    countries.add(country.getClassid());
                                                }
                                                Result res = new Result();
                                                res.setId(r.getId());
                                                List<Country> countryList = new ArrayList<>();
                                                for (CountrySbs country :
                                                        potentialNewCountries
                                                                .get()
                                                                .getCountrySet()) {
                                                    if (!countries.contains(country.getClassid())) {
                                                        countryList.add(
                                                                getCountry(
                                                                        country.getClassid(),
                                                                        country.getClassname()));
                                                    }
                                                }
                                                res.setCountry(countryList);
                                                r.mergeFrom(res);
                                            }
                                            return r;
                                        },
                                Encoders.bean(resultClazz));

        log.info("Saving graph table to path: {}", outputPath);
        // log.info("number of saved recordsa: {}", new_table.count());
        new_table.toJSON().write().option("compression", "gzip").text(outputPath);
        //                    .toJavaRDD()
        //                    .map(r -> OBJECT_MAPPER.writeValueAsString(r))
        //                    .saveAsTextFile(outputPath , GzipCodec.class);

    }

    private static <R extends Result> Dataset<Row> getPotentialResultToUpdate(
            SparkSession spark,
            String inputPath,
            Class<R> resultClazz,
            Broadcast<Dataset<DatasourceCountry>> broadcast_datasourcecountryassoc) {

        Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);
        result.createOrReplaceTempView("result");
        // log.info("number of results: {}", result.count());
        createCfHbforresult(spark);
        return countryPropagationAssoc(spark, broadcast_datasourcecountryassoc);
    }

    //    private static void createCfHbforresult(SparkSession spark) {
    //        String query;
    //        query = "SELECT id, inst.collectedfrom.key cf , inst.hostedby.key hb " +
    //                "FROM ( SELECT id, instance " +
    //                "FROM result " +
    //                " WHERE datainfo.deletedbyinference = false)  ds " +
    //                "LATERAL VIEW EXPLODE(instance) i AS inst";
    //        Dataset<Row> cfhb = spark.sql(query);
    //        cfhb.createOrReplaceTempView("cfhb");
    //        //log.info("cfhb_number : {}", cfhb.count());
    //    }

    private static Dataset<Row> countryPropagationAssoc(
            SparkSession spark,
            Broadcast<Dataset<DatasourceCountry>> broadcast_datasourcecountryassoc) {

        Dataset<DatasourceCountry> datasource_country = broadcast_datasourcecountryassoc.value();
        datasource_country.createOrReplaceTempView("datasource_country");
        log.info("datasource_country number : {}", datasource_country.count());

        String query =
                "SELECT id resultId, collect_set(country) countrySet "
                        + "FROM ( SELECT id, country "
                        + "FROM datasource_country "
                        + "JOIN cfhb "
                        + " ON cf = dataSourceId     "
                        + "UNION ALL "
                        + "SELECT id , country     "
                        + "FROM datasource_country "
                        + "JOIN cfhb "
                        + " ON hb = dataSourceId   ) tmp "
                        + "GROUP BY id";
        Dataset<Row> potentialUpdates = spark.sql(query);
        // log.info("potential update number : {}", potentialUpdates.count());
        return potentialUpdates;
    }

    private static Dataset<DatasourceCountry> readAssocDatasourceCountry(
            SparkSession spark, String relationPath) {
        return spark.read()
                .textFile(relationPath)
                .map(
                        value -> OBJECT_MAPPER.readValue(value, DatasourceCountry.class),
                        Encoders.bean(DatasourceCountry.class));
    }

    private static void writeUpdates(
            Dataset<ResultCountrySet> potentialUpdates, String outputPath) {
        potentialUpdates
                .toJSON()
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "gzip")
                .text(outputPath);
        //                map(u -> OBJECT_MAPPER.writeValueAsString(u))
        //                .saveAsTextFile(outputPath, GzipCodec.class);
    }
}
