
package eu.dnetlib.dhp.collection.plugin.zenodo;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.common.collection.CollectorException;

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
