
package eu.dnetlib.dhp.broker.oa.samples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.broker.model.ConditionParams;
import eu.dnetlib.dhp.broker.model.MapCondition;
import eu.dnetlib.dhp.broker.oa.util.SubscriptionUtils;

@Disabled
public class SimpleVariableJobTest {

	private static final Logger log = LoggerFactory.getLogger(SimpleVariableJobTest.class);

	private static Path workingDir;

	private static SparkSession spark;

	private final static List<String> inputList = new ArrayList<>();

	private static final Map<String, Map<String, List<ConditionParams>>> staticMap = new HashMap<>();

	@BeforeAll
	public static void beforeAll() throws IOException {

		workingDir = Files.createTempDirectory(SimpleVariableJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		final SparkConf conf = new SparkConf();
		conf.setAppName(SimpleVariableJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		// conf.set("spark.sql.warehouse.dir", workingDir.toString());
		// conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SimpleVariableJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();

		for (int i = 0; i < 1_000_000; i++) {
			inputList.add("record " + i);
		}
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void testSimpleVariableJob() throws Exception {
		final Map<String, Map<String, List<ConditionParams>>> map = fillMap();

		final long n = spark
			.createDataset(inputList, Encoders.STRING())
			.filter((FilterFunction<String>)  s -> filter(map.get(s)))
			.map((MapFunction<String, String>) String::toLowerCase, Encoders.STRING())
			.count();

		System.out.println(n);
	}

	@Test
	public void testSimpleVariableJob_static() throws Exception {

		staticMap.putAll(fillMap());

		final long n = spark
			.createDataset(inputList, Encoders.STRING())
			.filter((FilterFunction<String>) s -> filter(staticMap.get(s)))
			.map((MapFunction<String, String>) String::toLowerCase, Encoders.STRING())
			.count();

		System.out.println(n);
	}

	private static Map<String, Map<String, List<ConditionParams>>> fillMap()
		throws JsonParseException, JsonMappingException, IOException {
		final String s = "[{\"field\":\"targetDatasourceName\",\"fieldType\":\"STRING\",\"operator\":\"EXACT\",\"listParams\":[{\"value\":\"reposiTUm\"}]},{\"field\":\"trust\",\"fieldType\":\"FLOAT\",\"operator\":\"RANGE\",\"listParams\":[{\"value\":\"0\",\"otherValue\":\"1\"}]}]";

		final ObjectMapper mapper = new ObjectMapper();
		final List<MapCondition> list = mapper
			.readValue(s, mapper.getTypeFactory().constructCollectionType(List.class, MapCondition.class));
		final Map<String, List<ConditionParams>> conditions = list
			.stream()
			.filter(mc -> !mc.getListParams().isEmpty())
			.collect(Collectors.toMap(MapCondition::getField, MapCondition::getListParams));

		final Map<String, Map<String, List<ConditionParams>>> map = new HashMap<>();
		inputList.forEach(i -> map.put(i, conditions));
		return map;
	}

	private static boolean filter(final Map<String, List<ConditionParams>> conditions) {
		if (conditions.containsKey("targetDatasourceName")
			&& !SubscriptionUtils
				.verifyExact("reposiTUm", conditions.get("targetDatasourceName").get(0).getValue())) {
			return false;
		}
		return true;
	}

}
