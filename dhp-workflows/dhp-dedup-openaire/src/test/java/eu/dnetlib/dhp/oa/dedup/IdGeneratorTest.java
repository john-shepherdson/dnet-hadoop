
package eu.dnetlib.dhp.oa.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.*;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.oa.dedup.model.PidType;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IdGeneratorTest {

	private static List<Identifier> bestIds;
	private static List<Tuple2<String, Publication>> pubs;

	private static List<Identifier> bestIds2;
	private static List<Identifier> bestIds3;

	private static String testEntityBasePath;

	private static SimpleDateFormat sdf;
	private static Date baseDate;

	@BeforeAll
	public static void setUp() throws Exception {

		sdf = new SimpleDateFormat("yyyy-MM-dd");
		baseDate = sdf.parse("2000-01-01");

		bestIds = new ArrayList<>();
		bestIds2 = Lists
			.newArrayList(
				new Identifier(pid("pid1", "original", "original"), baseDate, PidType.original,
					keyValue("key", "value"), EntityType.publication, "50|originalID1"),
				new Identifier(pid("pid2", "original", "original"), baseDate, PidType.original,
					keyValue("key", "value"), EntityType.publication, "50|originalID2"),
				new Identifier(pid("pid3", "original", "original"), baseDate, PidType.original,
					keyValue("key", "value"), EntityType.publication, "50|originalID3"));
		bestIds3 = Lists
			.newArrayList(
				new Identifier(pid("pid1", "original", "original"), baseDate, PidType.original,
					keyValue("key", "value"), EntityType.publication, "50|originalID1"),
				new Identifier(pid("pid2", "doi", "doi"), baseDate, PidType.doi, keyValue("key", "value"),
					EntityType.publication, "50|originalID2"),
				new Identifier(pid("pid3", "original", "original"), baseDate, PidType.original,
					keyValue("key", "value"), EntityType.publication, "50|originalID3"));

		testEntityBasePath = Paths
			.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/json").toURI())
			.toFile()
			.getAbsolutePath();

		pubs = readSample(testEntityBasePath + "/publication_idgeneration.json", Publication.class);

	}

	@Test
	@Order(1)
	public void bestPidToIdentifierTest() {

		List<String> typesForAssertions = Lists
			.newArrayList(PidType.pmc.toString(), PidType.doi.toString(), PidType.doi.toString());

		for (Tuple2<String, Publication> pub : pubs) {
			List<Identifier> ids = IdGenerator.bestPidToIdentifier(pub._2());
			assertEquals(typesForAssertions.get(pubs.indexOf(pub)), ids.get(0).getPid().getQualifier().getClassid());
			bestIds.addAll(ids);
		}
	}

	@Test
	@Order(2)
	public void generateIdTest1() {
		String id1 = IdGenerator.generate(bestIds, "50|defaultID");

		System.out
			.println("id list 1 = " + bestIds.stream().map(i -> i.getPid().getValue()).collect(Collectors.toList()));

		assertEquals("50|dedup_wf_001::9c5cfbf993d38476e0f959a301239719", id1);
	}

	@Test
	public void generateIdTest2() {
		String id1 = IdGenerator.generate(bestIds2, "50|defaultID");
		String id2 = IdGenerator.generate(bestIds3, "50|defaultID");

		System.out
			.println("id list 2 = " + bestIds2.stream().map(i -> i.getPid().getValue()).collect(Collectors.toList()));
		System.out.println("winner 2 = " + id1);
		System.out
			.println("id list 3 = " + bestIds3.stream().map(i -> i.getPid().getValue()).collect(Collectors.toList()));
		System.out.println("winner 3 = " + id2);

		assertEquals("50|dedup_wf_001::2c56cc1914bffdb30fdff354e0099612", id1);
		assertEquals("50|dedup_doi___::128ead3ed8d9ecf262704b6fcf592b8d", id2);
	}

	public static <T> List<Tuple2<String, T>> readSample(String path, Class<T> clazz) {
		List<Tuple2<String, T>> res = new ArrayList<>();
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(path));
			String line = reader.readLine();
			while (line != null) {
				res
					.add(
						new Tuple2<>(
							MapDocumentUtil.getJPathString("$.id", line),
							new ObjectMapper().readValue(line, clazz)));
				// read next line
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return res;
	}

	public static StructuredProperty pid(String pid, String classid, String classname) {

		StructuredProperty sp = new StructuredProperty();
		sp.setValue(pid);
		Qualifier q = new Qualifier();
		q.setSchemeid(classid);
		q.setSchemename(classname);
		q.setClassname(classname);
		q.setClassid(classid);
		sp.setQualifier(q);
		return sp;
	}

	public static List<KeyValue> keyValue(String key, String value) {

		KeyValue kv = new KeyValue();
		kv.setKey(key);
		kv.setValue(value);
		return Lists.newArrayList(kv);
	}
}
