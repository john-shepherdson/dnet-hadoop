package eu.dnetlib.dhp.bypassactionset;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.bypassactionset.model.FOSDataModel;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.countrypropagation.CountryPropagationJobTest;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;

import java.nio.file.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetFOSTest {
    private static final Logger log = LoggerFactory.getLogger(GetFOSTest.class);

    private static Path workingDir;
    private static SparkSession spark;
    private static LocalFileSystem fs;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files.createTempDirectory(CountryPropagationJobTest.class.getSimpleName());

        fs = FileSystem.getLocal(new Configuration());
        log.info("using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(GetFOSTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .appName(CountryPropagationJobTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }
    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }


    @Test
    void getFOSFileTest() throws CollectorException, IOException, ClassNotFoundException {

        final String sourcePath = getClass()
                .getResource("/eu/dnetlib/dhp/bypassactionset/fos/h2020_fos_sbs.csv")
                .getPath();
        final String outputPath = workingDir.toString() + "/fos.json";




        new GetFOSData()
                .doRewrite(sourcePath, outputPath, "eu.dnetlib.dhp.bypassactionset.FOSDataModel", '\t',fs );

        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new org.apache.hadoop.fs.Path(outputPath))));

        String line;
        int count = 0;
        while ((line = in.readLine()) != null) {
            FOSDataModel fos = new ObjectMapper().readValue(line, FOSDataModel.class);

            System.out.println(new ObjectMapper().writeValueAsString(fos));
            count += 1;
        }

        assertEquals(38, count);


    }


    @Test
    void distributeDoiTest() throws Exception{
        final String sourcePath = getClass()
                .getResource("/eu/dnetlib/dhp/bypassactionset/fos/fos.json")
                .getPath();

        DistributeFOSSparkJob
                .main(
                        new String[] {
                                "--isSparkSessionManaged", Boolean.FALSE.toString(),
                                "--sourcePath", sourcePath,

                                "-outputPath", workingDir.toString() + "/distribute"

                        });

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<FOSDataModel> tmp = sc
                .textFile(workingDir.toString() + "/distribute")
                .map(item -> OBJECT_MAPPER.readValue(item, FOSDataModel.class));


        assertEquals(50, tmp.count());
        assertEquals(1, tmp.filter(row -> row.getDoi().equals("10.3390/s18072310")).count());
        assertEquals("engineering and technology", tmp.filter(r -> r.getDoi().equals("10.3390/s18072310")).collect().get(0).getLevel1());
        assertEquals("nano-technology", tmp.filter(r -> r.getDoi().equals("10.3390/s18072310")).collect().get(0).getLevel2());
        assertEquals("nanoscience & nanotechnology", tmp.filter(r -> r.getDoi().equals("10.3390/s18072310")).collect().get(0).getLevel3());

        assertEquals(1, tmp.filter(row -> row.getDoi().equals("10.1111/1365-2656.12831")).count());
        assertEquals("social sciences", tmp.filter(r -> r.getDoi().equals("10.1111/1365-2656.12831")).collect().get(0).getLevel1());
        assertEquals("psychology and cognitive sciences", tmp.filter(r -> r.getDoi().equals("10.1111/1365-2656.12831")).collect().get(0).getLevel2());
        assertEquals("NULL", tmp.filter(r -> r.getDoi().equals("10.1111/1365-2656.12831")).collect().get(0).getLevel3());

//        {"doi":"10.1111/1365-2656.12831\u000210.17863/cam.24369","level1":"social sciences","level2":"psychology and cognitive sciences","level3":"NULL"}

    }

    /**
     * @Test
     *        void testCountryPropagationSoftware() throws Exception {
     * 		final String sourcePath = getClass()
     * 			.getResource("/eu/dnetlib/dhp/countrypropagation/sample/software")
     * 			.getPath();
     * 		final String preparedInfoPath = getClass()
     * 			.getResource("/eu/dnetlib/dhp/countrypropagation/preparedInfo")
     * 			.getPath();
     * 		SparkCountryPropagationJob
     * 			.main(
     * 				new String[] {
     * 					"--isSparkSessionManaged", Boolean.FALSE.toString(),
     * 					"--sourcePath", sourcePath,
     * 					"-saveGraph", "true",
     * 					"-resultTableName", Software.class.getCanonicalName(),
     * 					"-outputPath", workingDir.toString() + "/software",
     * 					"-preparedInfoPath", preparedInfoPath
     *                });
     *
     * 		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
     *
     * 		JavaRDD<Software> tmp = sc
     * 			.textFile(workingDir.toString() + "/software")
     * 			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));
     *
     * 		// tmp.map(s -> new Gson().toJson(s)).foreach(s -> System.out.println(s));
     *
     * 		Assertions.assertEquals(10, tmp.count());
     *
     * 		Dataset<Software> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Software.class));
     *
     * 		Assertions.assertEquals(6, verificationDs.filter("size(country) > 0").count());
     * 		Assertions.assertEquals(3, verificationDs.filter("size(country) = 1").count());
     * 		Assertions.assertEquals(3, verificationDs.filter("size(country) = 2").count());
     * 		Assertions.assertEquals(0, verificationDs.filter("size(country) > 2").count());
     *
     * 		Dataset<String> countryExploded = verificationDs
     * 			.flatMap(
     * 				(FlatMapFunction<Software, Country>) row -> row.getCountry().iterator(), Encoders.bean(Country.class))
     * 			.map((MapFunction<Country, String>) Qualifier::getClassid, Encoders.STRING());
     *
     * 		Assertions.assertEquals(9, countryExploded.count());
     *
     * 		Assertions.assertEquals(1, countryExploded.filter("value = 'FR'").count());
     * 		Assertions.assertEquals(1, countryExploded.filter("value = 'TR'").count());
     * 		Assertions.assertEquals(2, countryExploded.filter("value = 'IT'").count());
     * 		Assertions.assertEquals(1, countryExploded.filter("value = 'US'").count());
     * 		Assertions.assertEquals(1, countryExploded.filter("value = 'MX'").count());
     * 		Assertions.assertEquals(1, countryExploded.filter("value = 'CH'").count());
     * 		Assertions.assertEquals(2, countryExploded.filter("value = 'JP'").count());
     *
     * 		Dataset<Tuple2<String, String>> countryExplodedWithCountryclassid = verificationDs
     * 			.flatMap((FlatMapFunction<Software, Tuple2<String, String>>) row -> {
     * 				List<Tuple2<String, String>> prova = new ArrayList<>();
     * 				List<Country> countryList = row.getCountry();
     * 				countryList
     * 					.forEach(
     * 						c -> prova
     * 							.add(
     * 								new Tuple2<>(
     * 									row.getId(), c.getClassid())));
     * 				return prova.iterator();
     *            }, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
     *
     * 		Assertions.assertEquals(9, countryExplodedWithCountryclassid.count());
     *
     * 		countryExplodedWithCountryclassid.show(false);
     * 		Assertions
     * 			.assertEquals(
     * 				1,
     * 				countryExplodedWithCountryclassid
     * 					.filter(
     * 						"_1 = '50|od______1582::6e7a9b21a2feef45673890432af34244' and _2 = 'FR' ")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				1,
     * 				countryExplodedWithCountryclassid
     * 					.filter(
     * 						"_1 = '50|dedup_wf_001::40ea2f24181f6ae77b866ebcbffba523' and _2 = 'TR' ")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				2,
     * 				countryExplodedWithCountryclassid
     * 					.filter(
     * 						"_1 = '50|od______1106::2b7ca9726230be8e862be224fd463ac4' and (_2 = 'IT' or _2 = 'MX') ")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				2,
     * 				countryExplodedWithCountryclassid
     * 					.filter(
     * 						"_1 = '50|od_______935::46a0ad9964171c3dd13373f5427b9a1c' and (_2 = 'IT' or _2 = 'US') ")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				1,
     * 				countryExplodedWithCountryclassid
     * 					.filter(
     * 						"_1 = '50|dedup_wf_001::b67bc915603fc01e445f2b5888ba7218' and _2 = 'JP'")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				2,
     * 				countryExplodedWithCountryclassid
     * 					.filter(
     * 						"_1 = '50|od_______109::f375befa62a741e9250e55bcfa88f9a6' and (_2 = 'CH' or _2 = 'JP') ")
     * 					.count());
     *
     * 		Dataset<Tuple2<String, String>> countryExplodedWithCountryclassname = verificationDs
     * 			.flatMap(
     * 				(FlatMapFunction<Software, Tuple2<String, String>>) row -> {
     * 					List<Tuple2<String, String>> prova = new ArrayList<>();
     * 					List<Country> countryList = row.getCountry();
     * 					countryList
     * 						.forEach(
     * 							c -> prova
     * 								.add(
     * 									new Tuple2<>(
     * 										row.getId(),
     * 										c.getClassname())));
     * 					return prova.iterator();
     *                },
     * 				Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
     *
     * 		countryExplodedWithCountryclassname.show(false);
     * 		Assertions
     * 			.assertEquals(
     * 				1,
     * 				countryExplodedWithCountryclassname
     * 					.filter(
     * 						"_1 = '50|od______1582::6e7a9b21a2feef45673890432af34244' and _2 = 'France' ")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				1,
     * 				countryExplodedWithCountryclassname
     * 					.filter(
     * 						"_1 = '50|dedup_wf_001::40ea2f24181f6ae77b866ebcbffba523' and _2 = 'Turkey' ")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				2,
     * 				countryExplodedWithCountryclassname
     * 					.filter(
     * 						"_1 = '50|od______1106::2b7ca9726230be8e862be224fd463ac4' and (_2 = 'Italy' or _2 = 'Mexico') ")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				2,
     * 				countryExplodedWithCountryclassname
     * 					.filter(
     * 						"_1 = '50|od_______935::46a0ad9964171c3dd13373f5427b9a1c' and (_2 = 'Italy' or _2 = 'United States') ")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				1,
     * 				countryExplodedWithCountryclassname
     * 					.filter(
     * 						"_1 = '50|dedup_wf_001::b67bc915603fc01e445f2b5888ba7218' and _2 = 'Japan' ")
     * 					.count());
     * 		Assertions
     * 			.assertEquals(
     * 				2,
     * 				countryExplodedWithCountryclassname
     * 					.filter(
     * 						"_1 = '50|od_______109::f375befa62a741e9250e55bcfa88f9a6' and (_2 = 'Switzerland' or _2 = 'Japan') ")
     * 					.count());
     *
     * 		Dataset<Tuple2<String, String>> countryExplodedWithCountryProvenance = verificationDs
     * 			.flatMap(
     * 				(FlatMapFunction<Software, Tuple2<String, String>>) row -> {
     * 					List<Tuple2<String, String>> prova = new ArrayList<>();
     * 					List<Country> countryList = row.getCountry();
     * 					countryList
     * 						.forEach(
     * 							c -> prova
     * 								.add(
     * 									new Tuple2<>(
     * 										row.getId(),
     * 										c
     * 											.getDataInfo()
     * 											.getInferenceprovenance())));
     * 					return prova.iterator();
     *                },
     * 				Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
     *
     * 		Assertions
     * 			.assertEquals(
     * 				7, countryExplodedWithCountryProvenance.filter("_2 = 'propagation'").count());
     *    }
     */
}
