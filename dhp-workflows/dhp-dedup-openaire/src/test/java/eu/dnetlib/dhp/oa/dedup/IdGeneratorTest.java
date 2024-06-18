
package eu.dnetlib.dhp.oa.dedup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.oa.dedup.model.Identifier;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IdGeneratorTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	private static List<Identifier<Publication>> bestIds;
	private static List<Identifier<Publication>> bestIds2;
	private static List<Identifier<Publication>> bestIds3;

	private static List<Identifier<Organization>> bestIdsOrg;

	private static String testEntityBasePath;

	@BeforeAll
	public static void setUp() throws Exception {
		testEntityBasePath = Paths
			.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/json").toURI())
			.toFile()
			.getAbsolutePath();

		bestIds = createBestIds(testEntityBasePath + "/publication_idgeneration.json", Publication.class);
		bestIds2 = createBestIds(testEntityBasePath + "/publication_idgeneration2.json", Publication.class);
		bestIds3 = createBestIds(testEntityBasePath + "/publication_idgeneration3.json", Publication.class);

		bestIdsOrg = createBestIds(testEntityBasePath + "/organization_idgeneration.json", Organization.class);
	}

	@Test
	void generateIdTest1() {
		String id1 = IdGenerator.generate(bestIds, "50|defaultID");

		System.out
			.println("id list 1 = " + bestIds.stream().map(i -> i.getOriginalID()).collect(Collectors.toList()));

		assertEquals("50|doi_dedup___::0968af610a356656706657e4f234b340", id1);
	}

	@Test
	void generateIdTest2() {
		String id1 = IdGenerator.generate(bestIds2, "50|defaultID");
		String id2 = IdGenerator.generate(bestIds3, "50|defaultID");

		System.out
			.println("id list 2 = " + bestIds2.stream().map(i -> i.getOriginalID()).collect(Collectors.toList()));
		System.out.println("winner 2 = " + id1);
		System.out
			.println("id list 3 = " + bestIds3.stream().map(i -> i.getOriginalID()).collect(Collectors.toList()));
		System.out.println("winner 3 = " + id2);

		assertEquals("50|doi_dedup___::1a77a3bba737f8b669dcf330ad3b37e2", id1);
		assertEquals("50|dedup_wf_002::345e5d1b80537b0d0e0a49241ae9e516", id2);
	}

	@Test
	void generateIdOrganizationTest() {
		String id1 = IdGenerator.generate(bestIdsOrg, "20|defaultID");

		assertEquals("20|openorgs____::599c15a70fcb03be6ba08f75f14d6076", id1);
	}

	protected static <T extends OafEntity> List<Identifier<T>> createBestIds(String path, Class<T> clazz) {
		final Stream<Identifier<T>> ids = readSample(path, clazz)
			.stream()
			.map(Tuple2::_2)
			.map(Identifier::newInstance);
		return ids.collect(Collectors.toList());
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
							OBJECT_MAPPER.readValue(line, clazz)));
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
		return OafMapperUtils.structuredProperty(pid, classid, classname, "", "", new DataInfo());
	}

	public static List<KeyValue> keyValue(String key, String value) {
		return Lists.newArrayList(OafMapperUtils.keyValue(key, value));
	}
}
