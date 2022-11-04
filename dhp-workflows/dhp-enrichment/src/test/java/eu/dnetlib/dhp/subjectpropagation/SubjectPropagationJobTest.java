
package eu.dnetlib.dhp.subjectpropagation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.PropagationConstant;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Subject;
import eu.dnetlib.dhp.subjecttoresultfromsemrel.SparkSubjectPropagationStep2;

/**
 * @author miriam.baglioni
 * @Date 06/10/22
 */
public class SubjectPropagationJobTest {
	private static final Logger log = LoggerFactory.getLogger(SubjectPropagationJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(SubjectPropagationJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(SubjectPropagationJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SubjectPropagationJobTest.class.getSimpleName())
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
		SparkSubjectPropagationStep2
			.main(
				new String[] {
					"-preparedPath", getClass()
						.getResource("/eu/dnetlib/dhp/subjectpropagation/preparedInfo")
						.getPath(),
					"-resultType", "publication",
					"-sourcePath", getClass()
						.getResource("/eu/dnetlib/dhp/subjectpropagation")
						.getPath(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication",
					"-workingPath", workingDir.toString() + "/working",
					"-outputPath", workingDir.toString()
				});

		// 50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98 should receive fake_fos, fake_sdg and fake_fos2
		// 50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba should receive fake_fos2

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Publication> tmp = sc
			.textFile(workingDir.toString() + "/publication")
			.map(item -> OBJECT_MAPPER.readValue(item, Publication.class));

		Assertions.assertEquals(4, tmp.count());

		Assertions
			.assertEquals(
				2, tmp
					.filter(
						r -> r
							.getSubject()
							.stream()
							.anyMatch(
								s -> s
									.getDataInfo()
									.getInferenceprovenance()
									.equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE)))
					.count());

		JavaRDD<Subject> sbjs = tmp
			.flatMap((FlatMapFunction<Publication, Subject>) r -> r.getSubject().iterator())
			.filter(
				s -> s.getDataInfo().getInferenceprovenance().equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE));

		Assertions.assertEquals(4, sbjs.count());
		Assertions
			.assertEquals(
				4, sbjs
					.filter(
						s -> s
							.getDataInfo()
							.getProvenanceaction()
							.getClassid()
							.equals(PropagationConstant.PROPAGATION_SUBJECT_RESULT_SEMREL_CLASS_ID))
					.count());
		Assertions
			.assertEquals(
				4,
				sbjs
					.filter(
						s -> s
							.getDataInfo()
							.getProvenanceaction()
							.getClassname()
							.equals(PropagationConstant.PROPAGATION_SUBJECT_RESULT_SEMREL_CLASS_NAME))
					.count());
		Assertions.assertEquals(3, sbjs.filter(s -> s.getQualifier().getClassid().equals("FOS")).count());
		Assertions
			.assertEquals(3, sbjs.filter(s -> s.getQualifier().getClassname().equals("Field of Science")).count());
		Assertions.assertEquals(1, sbjs.filter(s -> s.getQualifier().getClassid().equals("SDG")).count());
		Assertions
			.assertEquals(
				1, sbjs.filter(s -> s.getQualifier().getClassname().equals("Support and Development Goals")).count());

		Assertions
			.assertEquals(
				6,
				tmp
					.filter(r -> r.getId().equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98"))
					.first()
					.getSubject()
					.size());
		Assertions
			.assertEquals(
				3, tmp
					.filter(
						r -> r
							.getId()
							.equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98"))
					.first()
					.getSubject()
					.stream()
					.filter(
						s -> s
							.getDataInfo()
							.getInferenceprovenance()
							.equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE))
					.count());
		Assertions
			.assertEquals(
				3, tmp
					.filter(
						r -> r
							.getId()
							.equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98"))
					.first()
					.getSubject()
					.stream()
					.filter(
						s -> !s
							.getDataInfo()
							.getInferenceprovenance()
							.equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE))
					.count());
		Assertions
			.assertEquals(
				2, tmp
					.filter(
						r -> r
							.getId()
							.equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98"))
					.first()
					.getSubject()
					.stream()
					.filter(
						s -> s
							.getDataInfo()
							.getInferenceprovenance()
							.equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE) &&
							s.getQualifier().getClassid().equals("FOS"))
					.count());
		Assertions
			.assertEquals(
				1, tmp
					.filter(
						r -> r
							.getId()
							.equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98"))
					.first()
					.getSubject()
					.stream()
					.filter(
						s -> s
							.getDataInfo()
							.getInferenceprovenance()
							.equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE) &&
							s.getQualifier().getClassid().equals("SDG"))
					.count());

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getId().equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98"))
					.first()
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("fake_fos")));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getId().equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98"))
					.first()
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("fake_fos2")));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getId().equals("50|06cdd3ff4700::cd7711c65d518859f1d87056e2c45d98"))
					.first()
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("fake_sdg")));

		Assertions
			.assertEquals(
				6,
				tmp
					.filter(r -> r.getId().equals("50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba"))
					.first()
					.getSubject()
					.size());
		Assertions
			.assertEquals(
				1, tmp
					.filter(
						r -> r
							.getId()
							.equals("50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba"))
					.first()
					.getSubject()
					.stream()
					.filter(
						s -> s
							.getDataInfo()
							.getInferenceprovenance()
							.equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE))
					.count());
		Assertions
			.assertEquals(
				5, tmp
					.filter(
						r -> r
							.getId()
							.equals("50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba"))
					.first()
					.getSubject()
					.stream()
					.filter(
						s -> !s
							.getDataInfo()
							.getInferenceprovenance()
							.equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE))
					.count());
		Assertions
			.assertEquals(
				1, tmp
					.filter(
						r -> r
							.getId()
							.equals("50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba"))
					.first()
					.getSubject()
					.stream()
					.filter(
						s -> s
							.getDataInfo()
							.getInferenceprovenance()
							.equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE) &&
							s.getQualifier().getClassid().equals("FOS"))
					.count());
		Assertions
			.assertEquals(
				0, tmp
					.filter(
						r -> r
							.getId()
							.equals("50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba"))
					.first()
					.getSubject()
					.stream()
					.filter(
						s -> s
							.getDataInfo()
							.getInferenceprovenance()
							.equals(PropagationConstant.PROPAGATION_DATA_INFO_TYPE) &&
							s.getQualifier().getClassid().equals("SDG"))
					.count());

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getId().equals("50|06cdd3ff4700::93859bd27121c3ee7c6ee4bfb1790cba"))
					.first()
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("fake_fos2")));

	}
}
