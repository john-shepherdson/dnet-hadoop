package eu.dnetlib.dhp.collection.plugin.base;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.dom4j.Element;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.common.aggregation.AggregatorReport;

@ExtendWith(MockitoExtension.class)
public class BaseCollectorIteratorTest {

	@Test
	void testImportFile() throws Exception {

		long count = 0;

		final BaseCollectorIterator iterator = new BaseCollectorIterator("base-sample.tar", new AggregatorReport());

		while (iterator.hasNext()) {
			final Element record = iterator.next();
			// System.out.println(record.asXML());
			count++;
		}
		assertEquals(30000, count);
	}

}
