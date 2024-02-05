package eu.dnetlib.dhp.collection.plugin.base;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.util.Iterator;

import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.common.aggregation.AggregatorReport;

public class BaseCollectorIteratorTest {

	@Test
	void testImportFile() throws Exception {
		long count = 0;

		try (final InputStream is = getClass().getResourceAsStream("base-sample.tar")) {
			final Iterator<String> iterator = new BaseCollectorIterator(is, new AggregatorReport());
			while (iterator.hasNext()) {
				final String record = iterator.next();
				System.out.println(record);
				count++;
			}
		}

		assertEquals(30000, count);
	}

}
