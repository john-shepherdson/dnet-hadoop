
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;

public class ProduceTest {
	private static final Logger log = LoggerFactory.getLogger(ProduceTest.class);

	private static Path workingDir;
	private static SparkSession spark;
	private static LocalFileSystem fs;
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String ID_PREFIX = "50|doi_________";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(ProduceTest.class.getSimpleName());

		fs = FileSystem.getLocal(new Configuration());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(ProduceTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(ProduceTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void produceTestSubjects() throws Exception {

		JavaRDD<Result> tmp = getResultJavaRDD();

		List<Subject> sbjs = tmp
			.filter(row -> row.getSubject() != null && row.getSubject().size() > 0)
			.flatMap(row -> row.getSubject().iterator())
			.collect();

		sbjs.forEach(sbj -> Assertions.assertEquals("FOS", sbj.getQualifier().getClassid()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(
						"Fields of Science and Technology classification", sbj.getQualifier().getClassname()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(ModelConstants.DNET_SUBJECT_TYPOLOGIES, sbj.getQualifier().getSchemeid()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(ModelConstants.DNET_SUBJECT_TYPOLOGIES, sbj.getQualifier().getSchemename()));

		sbjs.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getDeletedbyinference()));
		sbjs.forEach(sbj -> Assertions.assertEquals(true, sbj.getDataInfo().getInferred()));
		sbjs.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getInvisible()));
		sbjs.forEach(sbj -> Assertions.assertEquals("", sbj.getDataInfo().getTrust()));
		sbjs.forEach(sbj -> Assertions.assertEquals("update", sbj.getDataInfo().getInferenceprovenance()));
		sbjs
			.forEach(
				sbj -> Assertions.assertEquals("subject:fos", sbj.getDataInfo().getProvenanceaction().getClassid()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals("Inferred by OpenAIRE", sbj.getDataInfo().getProvenanceaction().getClassname()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(
						ModelConstants.DNET_PROVENANCE_ACTIONS, sbj.getDataInfo().getProvenanceaction().getSchemeid()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(
						ModelConstants.DNET_PROVENANCE_ACTIONS,
						sbj.getDataInfo().getProvenanceaction().getSchemename()));
	}

	@Test
	void produceTestMeasuress() throws Exception {

		JavaRDD<Result> tmp = getResultJavaRDD();

		List<KeyValue> mes = tmp
			.filter(row -> row.getInstance() != null && row.getInstance().size() > 0)
			.flatMap(row -> row.getInstance().iterator())
			.flatMap(i -> i.getMeasures().iterator())
			.flatMap(m -> m.getUnit().iterator())
			.collect();

		mes.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getDeletedbyinference()));
		mes.forEach(sbj -> Assertions.assertEquals(true, sbj.getDataInfo().getInferred()));
		mes.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getInvisible()));
		mes.forEach(sbj -> Assertions.assertEquals("", sbj.getDataInfo().getTrust()));
		mes.forEach(sbj -> Assertions.assertEquals("update", sbj.getDataInfo().getInferenceprovenance()));
		mes
			.forEach(
				sbj -> Assertions.assertEquals("measure:bip", sbj.getDataInfo().getProvenanceaction().getClassid()));
		mes
			.forEach(
				sbj -> Assertions
					.assertEquals("Inferred by OpenAIRE", sbj.getDataInfo().getProvenanceaction().getClassname()));
		mes
			.forEach(
				sbj -> Assertions
					.assertEquals(
						ModelConstants.DNET_PROVENANCE_ACTIONS, sbj.getDataInfo().getProvenanceaction().getSchemeid()));
		mes
			.forEach(
				sbj -> Assertions
					.assertEquals(
						ModelConstants.DNET_PROVENANCE_ACTIONS,
						sbj.getDataInfo().getProvenanceaction().getSchemename()));
	}

	@Test
	void produceTest6Subjects() throws Exception {
		final String doi = "unresolved::10.3390/s18072310::doi";

		JavaRDD<Result> tmp = getResultJavaRDD();

		Assertions
			.assertEquals(
				6, tmp
					.filter(row -> row.getId().equals(doi))
					.collect()
					.get(0)
					.getSubject()
					.size());

		List<Subject> sbjs = tmp
			.filter(row -> row.getId().equals(doi))
			.flatMap(row -> row.getSubject().iterator())
			.collect();

		Assertions
			.assertEquals(
				true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("04 agricultural and veterinary sciences")));

		Assertions
			.assertEquals(
				true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("0404 agricultural biotechnology")));
		Assertions.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("040502 food science")));

		Assertions
			.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("03 medical and health sciences")));
		Assertions.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("0303 health sciences")));
		Assertions
			.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("030309 nutrition & dietetics")));

	}

	@Test
	void produceTest3Measures() throws Exception {
		final String doi = "unresolved::10.3390/s18072310::doi";
		JavaRDD<Result> tmp = getResultJavaRDD();

		tmp
			.filter(row -> row.getId().equals(doi))
			.foreach(r -> System.out.println(OBJECT_MAPPER.writeValueAsString(r)));
		Assertions
			.assertEquals(
				3, tmp
					.filter(row -> row.getId().equals(doi))
					.collect()
					.get(0)
					.getInstance()
					.get(0)
					.getMeasures()
					.size());

		List<Measure> measures = tmp
			.filter(row -> row.getId().equals(doi))
			.flatMap(row -> row.getInstance().iterator())
			.flatMap(inst -> inst.getMeasures().iterator())
			.collect();
		Assertions
			.assertEquals(
				"7.5597134689e-09", measures
					.stream()
					.filter(mes -> mes.getId().equals("influence"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"4.903880192", measures
					.stream()
					.filter(mes -> mes.getId().equals("popularity_alt"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"1.17977512835e-08", measures
					.stream()
					.filter(mes -> mes.getId().equals("popularity"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"10.3390/s18072310",
				tmp
					.filter(row -> row.getId().equals(doi))
					.collect()
					.get(0)
					.getInstance()
					.get(0)
					.getPid()
					.get(0)
					.getValue()
					.toLowerCase());

		Assertions
			.assertEquals(
				"doi",
				tmp
					.filter(row -> row.getId().equals(doi))
					.collect()
					.get(0)
					.getInstance()
					.get(0)
					.getPid()
					.get(0)
					.getQualifier()
					.getClassid());

		Assertions
			.assertEquals(
				"Digital Object Identifier",
				tmp
					.filter(row -> row.getId().equals(doi))
					.collect()
					.get(0)
					.getInstance()
					.get(0)
					.getPid()
					.get(0)
					.getQualifier()
					.getClassname());

	}

	@Test
	void produceTestMeasures() throws Exception {
		final String doi = "unresolved::10.3390/s18072310::doi";
		JavaRDD<Result> tmp = getResultJavaRDD();

		List<StructuredProperty> mes = tmp
			.filter(row -> row.getInstance() != null && row.getInstance().size() > 0)
			.flatMap(row -> row.getInstance().iterator())
			.flatMap(i -> i.getPid().iterator())
			.collect();

		Assertions.assertEquals(86, mes.size());

		tmp
			.filter(row -> row.getInstance() != null && row.getInstance().size() > 0)
			.foreach(
				e -> Assertions.assertEquals("sysimport:enrich", e.getDataInfo().getProvenanceaction().getClassid()));

	}

	@Test
	void produceTestSomeNumbers() throws Exception {

		final String doi = "unresolved::10.3390/s18072310::doi";
		JavaRDD<Result> tmp = getResultJavaRDD();

		Assertions.assertEquals(105, tmp.count());

		Assertions.assertEquals(1, tmp.filter(row -> row.getId().equals(doi)).count());

		Assertions
			.assertEquals(
				19, tmp
					.filter(row -> !row.getId().equals(doi))
					.filter(row -> row.getSubject() != null)
					.count());

		Assertions
			.assertEquals(
				85,
				tmp
					.filter(row -> !row.getId().equals(doi))
					.filter(r -> r.getInstance() != null && r.getInstance().size() > 0)
					.count());

	}

	private JavaRDD<Result> getResultJavaRDD() throws Exception {
		final String bipPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/bip/bip.json")
			.getPath();

		PrepareBipFinder
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", bipPath,
					"--outputPath", workingDir.toString() + "/work"

				});
		final String fosPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/fos/fos.json")
			.getPath();

		PrepareFOSSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", fosPath,
					"-outputPath", workingDir.toString() + "/work"
				});

		SparkSaveUnresolved.main(new String[] {
			"--isSparkSessionManaged", Boolean.FALSE.toString(),
			"--sourcePath", workingDir.toString() + "/work",

			"-outputPath", workingDir.toString() + "/unresolved"

		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		return sc
			.textFile(workingDir.toString() + "/unresolved")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));
	}

	@Test
	public JavaRDD<Result> getResultFosJavaRDD() throws Exception {

		final String fosPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/fos/fos_sbs_2.json")
			.getPath();

		PrepareFOSSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", fosPath,
					"-outputPath", workingDir.toString() + "/work"
				});

		SparkSaveUnresolved.main(new String[] {
			"--isSparkSessionManaged", Boolean.FALSE.toString(),
			"--sourcePath", workingDir.toString() + "/work",

			"-outputPath", workingDir.toString() + "/unresolved"

		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Result> tmp = sc
			.textFile(workingDir.toString() + "/unresolved")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));
		tmp.foreach(r -> System.out.println(new ObjectMapper().writeValueAsString(r)));

		return tmp;

	}

	@Test
	void prepareTest5Subjects() throws Exception {
		final String doi = "unresolved::10.1063/5.0032658::doi";

		JavaRDD<Result> tmp = getResultJavaRDD();

		Assertions.assertEquals(1, tmp.filter(row -> row.getId().equals(doi)).count());

		Assertions
			.assertEquals(
				5, tmp
					.filter(row -> row.getId().equals(doi))
					.collect()
					.get(0)
					.getSubject()
					.size());

		List<Subject> sbjs = tmp
			.filter(row -> row.getId().equals(doi))
			.flatMap(row -> row.getSubject().iterator())
			.collect();

		Assertions
			.assertEquals(
				true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("01 natural sciences")));
		Assertions.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("0103 physical sciences")));

		Assertions
			.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("010304 chemical physics")));
		Assertions.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("0104 chemical sciences")));
		Assertions
			.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("010402 general chemistry")));

	}

	private JavaRDD<Result> getResultJavaRDDPlusSDG() throws Exception {
		final String bipPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/bip/bip.json")
			.getPath();

		PrepareBipFinder
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", bipPath,
					"--outputPath", workingDir.toString() + "/work"

				});
		final String fosPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/fos/fos.json")
			.getPath();

		PrepareFOSSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", fosPath,
					"-outputPath", workingDir.toString() + "/work"
				});

		final String sdgPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/sdg/sdg.json")
			.getPath();

		PrepareSDGSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sdgPath,
					"-outputPath", workingDir.toString() + "/work"
				});

		SparkSaveUnresolved.main(new String[] {
			"--isSparkSessionManaged", Boolean.FALSE.toString(),
			"--sourcePath", workingDir.toString() + "/work",

			"-outputPath", workingDir.toString() + "/unresolved"

		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		return sc
			.textFile(workingDir.toString() + "/unresolved")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));
	}

	@Test
	void produceTestSomeNumbersWithSDG() throws Exception {

		final String doi = "unresolved::10.3390/s18072310::doi";
		JavaRDD<Result> tmp = getResultJavaRDDPlusSDG();

		Assertions.assertEquals(136, tmp.count());

		Assertions.assertEquals(1, tmp.filter(row -> row.getId().equals(doi)).count());

		Assertions
			.assertEquals(
				50, tmp
					.filter(row -> !row.getId().equals(doi))
					.filter(row -> row.getSubject() != null)
					.count());

		Assertions
			.assertEquals(
				85,
				tmp
					.filter(row -> !row.getId().equals(doi))
					.filter(r -> r.getInstance() != null && r.getInstance().size() > 0)
					.count());

	}

	@Test
	void produceTest7Subjects() throws Exception {
		final String doi = "unresolved::10.3390/s18072310::doi";

		JavaRDD<Result> tmp = getResultJavaRDDPlusSDG();

		Assertions
			.assertEquals(
				7, tmp
					.filter(row -> row.getId().equals(doi))
					.collect()
					.get(0)
					.getSubject()
					.size());

		List<Subject> sbjs = tmp
			.filter(row -> row.getId().equals(doi))
			.flatMap(row -> row.getSubject().iterator())
			.collect();

		Assertions
			.assertEquals(
				true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("04 agricultural and veterinary sciences")));

		Assertions
			.assertEquals(
				true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("0404 agricultural biotechnology")));
		Assertions.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("040502 food science")));

		Assertions
			.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("03 medical and health sciences")));
		Assertions.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("0303 health sciences")));
		Assertions
			.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("030309 nutrition & dietetics")));
		Assertions
			.assertEquals(true, sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("1. No poverty")));

	}

	@Test
	void produceTestSubjectsWithSDG() throws Exception {

		JavaRDD<Result> tmp = getResultJavaRDDPlusSDG();

		List<Subject> sbjs_sdg = tmp
			.filter(row -> row.getSubject() != null && row.getSubject().size() > 0)
			.flatMap(row -> row.getSubject().iterator())
			.filter(sbj -> sbj.getQualifier().getClassid().equals(Constants.SDG_CLASS_ID))
			.collect();

		sbjs_sdg.forEach(sbj -> Assertions.assertEquals("SDG", sbj.getQualifier().getClassid()));
		sbjs_sdg
			.forEach(
				sbj -> Assertions
					.assertEquals(
						"Sustainable Development Goals", sbj.getQualifier().getClassname()));
		sbjs_sdg
			.forEach(
				sbj -> Assertions
					.assertEquals(ModelConstants.DNET_SUBJECT_TYPOLOGIES, sbj.getQualifier().getSchemeid()));
		sbjs_sdg
			.forEach(
				sbj -> Assertions
					.assertEquals(ModelConstants.DNET_SUBJECT_TYPOLOGIES, sbj.getQualifier().getSchemename()));

		sbjs_sdg.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getDeletedbyinference()));
		sbjs_sdg.forEach(sbj -> Assertions.assertEquals(true, sbj.getDataInfo().getInferred()));
		sbjs_sdg.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getInvisible()));
		sbjs_sdg.forEach(sbj -> Assertions.assertEquals("", sbj.getDataInfo().getTrust()));
		sbjs_sdg.forEach(sbj -> Assertions.assertEquals("update", sbj.getDataInfo().getInferenceprovenance()));
		sbjs_sdg
			.forEach(
				sbj -> Assertions.assertEquals("subject:sdg", sbj.getDataInfo().getProvenanceaction().getClassid()));
		sbjs_sdg
			.forEach(
				sbj -> Assertions
					.assertEquals("Inferred by OpenAIRE", sbj.getDataInfo().getProvenanceaction().getClassname()));
		sbjs_sdg
			.forEach(
				sbj -> Assertions
					.assertEquals(
						ModelConstants.DNET_PROVENANCE_ACTIONS, sbj.getDataInfo().getProvenanceaction().getSchemeid()));
		sbjs_sdg
			.forEach(
				sbj -> Assertions
					.assertEquals(
						ModelConstants.DNET_PROVENANCE_ACTIONS,
						sbj.getDataInfo().getProvenanceaction().getSchemename()));
	}

}
