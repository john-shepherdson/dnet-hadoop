
package eu.dnetlib.dhp.collection.plugin.zenodo;

import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ZenodoPluginCollectionTest {

	@Test
	public void testZenodoIterator() throws Exception {

		final GZIPInputStream gis = new GZIPInputStream(
			getClass().getResourceAsStream("/eu/dnetlib/dhp/collection/zenodo/zenodo.tar.gz"));
		try (ZenodoTarIterator it = new ZenodoTarIterator(gis)) {
			Assertions.assertTrue(it.hasNext());
			int i = 0;
			while (it.hasNext()) {
				Assertions.assertNotNull(it.next());
				i++;
			}
			Assertions.assertEquals(10, i);

		}
	}

}
