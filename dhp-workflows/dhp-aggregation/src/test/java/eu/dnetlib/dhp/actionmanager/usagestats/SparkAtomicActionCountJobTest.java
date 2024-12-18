
package eu.dnetlib.dhp.actionmanager.usagestats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
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

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;

public class SparkAtomicActionCountJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(SparkAtomicActionCountJobTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(SparkAtomicActionCountJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(SparkAtomicActionCountJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SparkAtomicActionCountJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testUsageStatsDb2() {
		String usageScoresPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/usagestats/test2")
			.getPath();

		SparkAtomicActionUsageJob.writeActionSet(spark, usageScoresPath, workingDir.toString() + "/actionSet");

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<AtomicAction> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet", Text.class, Text.class)
			.map(usm -> OBJECT_MAPPER.readValue(usm._2.getBytes(), AtomicAction.class));
		// .map(aa -> (Result) aa.getPayload());

		Assertions.assertEquals(7, tmp.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("50|")).count());
		Assertions.assertEquals(9, tmp.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("10|")).count());
		Assertions.assertEquals(9, tmp.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("40|")).count());

		tmp.foreach(r -> Assertions.assertEquals(2, ((OafEntity) r.getPayload()).getMeasures().size()));
		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(u -> Assertions.assertFalse(u.getDataInfo().getDeletedbyinference()))));
		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m.getUnit().stream().forEach(u -> Assertions.assertTrue(u.getDataInfo().getInferred()))));
		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(u -> Assertions.assertFalse(u.getDataInfo().getInvisible()))));

		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(
								u -> Assertions
									.assertEquals(
										"measure:usage_counts",
										u.getDataInfo().getProvenanceaction().getClassid()))));
		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(
								u -> Assertions
									.assertEquals(
										"Inferred by OpenAIRE",
										u.getDataInfo().getProvenanceaction().getClassname()))));

		tmp
			.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("40|"))
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(
								u -> Assertions
									.assertEquals(
										"count",
										u.getKey()))));

		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						r -> ((OafEntity) r.getPayload())
							.getId()
							.equals("50|dedup_wf_001::53575dc69e9ace947e02d47ecd54a7a6"))
					.count());

		OafEntity entity = (OafEntity) tmp
			.filter(
				aa -> ((OafEntity) aa.getPayload()).getId().equals("50|dedup_wf_001::53575dc69e9ace947e02d47ecd54a7a6"))
			.first()
			.getPayload();

		entity
			.getMeasures()
			.stream()
			.forEach(
				m -> Assertions.assertEquals(3, m.getUnit().size()));

		Measure downloads = entity
			.getMeasures()
			.stream()
			.filter(m -> m.getId().equals("downloads"))
			.findFirst()
			.get();

		Assertions
			.assertEquals(
				String.valueOf(0),
				downloads.getUnit().stream().filter(u -> u.getKey().equals("10|fake1")).findFirst().get().getValue());
		Assertions
			.assertEquals(
				String.valueOf(0),
				downloads.getUnit().stream().filter(u -> u.getKey().equals("10|fake2")).findFirst().get().getValue());
		Assertions
			.assertEquals(
				String.valueOf(1),
				downloads.getUnit().stream().filter(u -> u.getKey().equals("10|fake3")).findFirst().get().getValue());

		Measure views = entity
			.getMeasures()
			.stream()
			.filter(m -> m.getId().equals("views"))
			.findFirst()
			.get();

		Assertions
			.assertEquals(
				String.valueOf(5),
				views.getUnit().stream().filter(u -> u.getKey().equals("10|fake1")).findFirst().get().getValue());
		Assertions
			.assertEquals(
				String.valueOf(1),
				views.getUnit().stream().filter(u -> u.getKey().equals("10|fake2")).findFirst().get().getValue());
		Assertions
			.assertEquals(
				String.valueOf(3),
				views.getUnit().stream().filter(u -> u.getKey().equals("10|fake3")).findFirst().get().getValue());

		tmp
			.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("10|"))
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(
								u -> Assertions
									.assertEquals(
										"count",
										u.getKey()))));

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f1__________::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"5",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f1__________::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f11_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"1",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f11_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"2",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f12_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"6",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f12_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d1__________::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"5",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d1__________::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d11_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"1",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d11_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"2",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d12_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"6",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d12_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
	}

	@Test
	void testMatch() {
		String usageScoresPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/usagestats/test1")
			.getPath();

		SparkAtomicActionUsageJob.writeActionSet(spark, usageScoresPath, workingDir.toString() + "/actionSet");

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<AtomicAction> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet", Text.class, Text.class)
			.map(usm -> OBJECT_MAPPER.readValue(usm._2.getBytes(), AtomicAction.class));
		// .map(aa -> (Result) aa.getPayload());

		Assertions.assertEquals(9, tmp.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("50|")).count());
		Assertions.assertEquals(9, tmp.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("10|")).count());
		Assertions.assertEquals(9, tmp.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("40|")).count());

		tmp.foreach(r -> Assertions.assertEquals(2, ((OafEntity) r.getPayload()).getMeasures().size()));
		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(u -> Assertions.assertFalse(u.getDataInfo().getDeletedbyinference()))));
		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m.getUnit().stream().forEach(u -> Assertions.assertTrue(u.getDataInfo().getInferred()))));
		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(u -> Assertions.assertFalse(u.getDataInfo().getInvisible()))));

		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(
								u -> Assertions
									.assertEquals(
										"measure:usage_counts",
										u.getDataInfo().getProvenanceaction().getClassid()))));
		tmp
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(
								u -> Assertions
									.assertEquals(
										"Inferred by OpenAIRE",
										u.getDataInfo().getProvenanceaction().getClassname()))));

		tmp
			.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("40|"))
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(
								u -> Assertions
									.assertEquals(
										"count",
										u.getKey()))));

		tmp
			.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("50|"))
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(
								u -> Assertions
									.assertEquals(
										"10|fake1",
										u.getKey()))));

		tmp
			.filter(aa -> ((OafEntity) aa.getPayload()).getId().startsWith("10|"))
			.foreach(
				r -> ((OafEntity) r.getPayload())
					.getMeasures()
					.stream()
					.forEach(
						m -> m
							.getUnit()
							.stream()
							.forEach(
								u -> Assertions
									.assertEquals(
										"count",
										u.getKey()))));

		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						r -> ((OafEntity) r.getPayload())
							.getId()
							.equals("50|dedup_wf_001::53575dc69e9ace947e02d47ecd54a7a6"))
					.count());

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("50|dedup_wf_001::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"5",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("50|dedup_wf_001::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("50|doi_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"1",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("50|doi_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"2",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("50|doi_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"6",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("50|doi_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f1__________::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"5",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f1__________::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f11_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"1",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f11_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"2",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f12_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"6",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("40|f12_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d1__________::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"5",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d1__________::53575dc69e9ace947e02d47ecd54a7a6"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"0",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d11_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"1",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d11_________::17eda2ff77407538fbe5d3d719b9d1c0"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"2",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d12_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("downloads"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"6",
				tmp
					.map(r -> ((OafEntity) r.getPayload()))
					.filter(r -> r.getId().equals("10|d12_________::3085e4c6e051378ca6157fe7f0430c1f"))
					.collect()
					.get(0)
					.getMeasures()
					.stream()
					.filter(m -> m.getId().equals("views"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
	}
}
