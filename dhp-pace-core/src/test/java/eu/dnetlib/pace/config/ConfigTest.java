package eu.dnetlib.pace.config;


import eu.dnetlib.pace.AbstractPaceTest;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class ConfigTest extends AbstractPaceTest {

	private static Map<String, String> params;

	@BeforeAll
	public static void setup() {
		params = new HashMap<>();
		params.put("jpath_value", "$.value");
		params.put("jpath_classid", "$.qualifier.classid");

	}

	@Test
	public void dedupConfigSerializationTest() {
		final DedupConfig cfgFromClasspath = DedupConfig.load(readFromClasspath("organization.current.conf.json"));

		final String conf = cfgFromClasspath.toString();

		final DedupConfig cfgFromSerialization = DedupConfig.load(conf);

		assertEquals(cfgFromClasspath.toString(), cfgFromSerialization.toString());

		assertNotNull(cfgFromClasspath);
		assertNotNull(cfgFromSerialization);
	}

	@Test
	public void dedupConfigTest() {

		DedupConfig load = DedupConfig.load(readFromClasspath("organization.current.conf.json"));

		System.out.println(load.toString());
	}

	@Test
	public void initTranslationMapTest() {

		DedupConfig load = DedupConfig.load(readFromClasspath("organization.current.conf.json"));

		Map<String, String> translationMap = load.translationMap();

		System.out.println("translationMap = " + translationMap.size());

		for (String key: translationMap.keySet()) {
			if (translationMap.get(key).equals("key::1"))
				System.out.println("key = " + key);
		}
	}

	@Test
	public void emptyTranslationMapTest() {

		DedupConfig load = DedupConfig.load(readFromClasspath("organization.no_synonyms.conf.json"));

		assertEquals(0, load.getPace().translationMap().keySet().size());
	}

    @Test
    public  void testJPath()  {
        final String json = readFromClasspath("organization.json");

        final String jpath ="$.id";

        System.out.println("result = " + MapDocumentUtil.getJPathString(jpath, json));
    }

}
