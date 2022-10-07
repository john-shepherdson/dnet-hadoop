
package eu.dnetlib.dhp.subjectpropagation;

import static org.apache.spark.sql.functions.desc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.resulttocommunityfromsemrel.ResultToCommunityJobTest;
import eu.dnetlib.dhp.resulttocommunityfromsemrel.SparkResultToCommunityThroughSemRelJob;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Subject;
import eu.dnetlib.dhp.subjecttoresultfromsemrel.PrepareResultResultStep1;
import eu.dnetlib.dhp.subjecttoresultfromsemrel.ResultSubjectList;
import eu.dnetlib.dhp.subjecttoresultfromsemrel.SubjectInfo;

/**
 * @author miriam.baglioni
 * @Date 05/10/22
 */
public class SubjectPreparationJobTest {
	private static final Logger log = LoggerFactory.getLogger(SubjectPreparationJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(SubjectPreparationJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(SubjectPreparationJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SubjectPreparationJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testSparkSubjectToResultThroughSemRelJob() throws Exception {
		PrepareResultResultStep1
			.main(
				new String[] {
					"-allowedSemRel",
					"IsSupplementedBy;IsSupplementTo;IsPreviousVersionOf;IsNewVersionOf;IsIdenticalTo;Obsoletes;IsObsoletedBy;IsVersionOf",
					"-subjectlist", "fos;sdg",
					"-resultType", "publication",
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", getClass()
						.getResource("/eu/dnetlib/dhp/subjectpropagation")
						.getPath(),
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication",
					"-outputPath", workingDir.toString()

				});

		// 50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba fake_fos and fake_sdg IsVersionOf
		// 50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98
		// 50|06cdd3ff4700::ff21e3c55d527fa7db171137c5fd1f1f fake_fos2 obsoletes
		// 50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98
		// 50|355e65625b88::046477dc24819c5f1453166aa7bfb75e fake_fos2 isSupplementedBy
		// 50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98
		// 50|355e65625b88::046477dc24819c5f1453166aa7bfb75e fake_fos2 issupplementto
		// 50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultSubjectList> tmp = sc
			.textFile(workingDir.toString() + "/publication")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultSubjectList.class));

		Assertions.assertEquals(2, tmp.count());

		Assertions
			.assertEquals(
				1, tmp.filter(r -> r.getResId().equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98")).count());
		Assertions
			.assertEquals(
				1, tmp.filter(r -> r.getResId().equals("50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba")).count());

		List<SubjectInfo> sbjList = tmp
			.filter(r -> r.getResId().equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98"))
			.first()
			.getSubjectList();

		Assertions.assertEquals(3, sbjList.size());
		Assertions.assertEquals(1, sbjList.stream().filter(s -> s.getClassid().equals("sdg")).count());
		Assertions.assertEquals(2, sbjList.stream().filter(s -> s.getClassid().equals("fos")).count());

		Assertions
			.assertEquals(
				"fake_sdg",
				sbjList.stream().filter(s -> s.getClassid().equalsIgnoreCase("sdg")).findFirst().get().getValue());
		Assertions
			.assertTrue(
				sbjList
					.stream()
					.filter(s -> s.getClassid().equalsIgnoreCase("fos"))
					.anyMatch(s -> s.getValue().equals("fake_fos")));
		Assertions
			.assertTrue(
				sbjList
					.stream()
					.filter(s -> s.getClassid().equalsIgnoreCase("fos"))
					.anyMatch(s -> s.getValue().equals("fake_fos2")));

		sbjList = tmp
			.filter(r -> r.getResId().equals("50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba"))
			.first()
			.getSubjectList();

		Assertions.assertEquals(1, sbjList.size());
		Assertions.assertEquals("fos", sbjList.get(0).getClassid().toLowerCase());

		Assertions.assertEquals("fake_fos2", sbjList.get(0).getValue());

		tmp.foreach(s -> System.out.println(OBJECT_MAPPER.writeValueAsString(s)));
	}

}
