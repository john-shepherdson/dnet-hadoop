package eu.dnetlib.dhp.collection.plugin.base;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class BaseCollectorPluginTest {

	@Test
	void testFilterXml() throws Exception {
		final String xml = IOUtils.toString(getClass().getResourceAsStream("record.xml"));

		final Set<String> validIds = new HashSet<>(Arrays.asList("opendoar____::1234", "opendoar____::4567"));
		final Set<String> validTypes = new HashSet<>(Arrays.asList("1", "121"));
		final Set<String> validTypes2 = new HashSet<>(Arrays.asList("1", "11"));

		assertTrue(BaseCollectorPlugin.filterXml(xml, validIds, validTypes));
		assertTrue(BaseCollectorPlugin.filterXml(xml, validIds, new HashSet<>()));

		assertFalse(BaseCollectorPlugin.filterXml(xml, new HashSet<>(), validTypes));
		assertFalse(BaseCollectorPlugin.filterXml(xml, validIds, validTypes2));

	}

}
