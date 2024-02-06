package eu.dnetlib.dhp.collection.plugin.base;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.util.Iterator;

import org.dom4j.Element;
import org.junit.jupiter.api.Test;

public class BaseCollectorIteratorTest {

	@Test
	void testImportFile() throws Exception {
		long count = 0;

		try (final InputStream is = getClass().getResourceAsStream("base-sample.tar")) {
			final Iterator<Element> iterator = new BaseCollectorIterator(is);
			while (iterator.hasNext()) {
				final Element record = iterator.next();
				System.out.println(record.asXML());
				count++;
			}
		}

		assertEquals(30000, count);
	}

}
