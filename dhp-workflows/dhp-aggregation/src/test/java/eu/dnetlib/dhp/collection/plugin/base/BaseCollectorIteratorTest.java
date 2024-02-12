package eu.dnetlib.dhp.collection.plugin.base;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Attribute;
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

		final Map<String, Map<String, String>> collections = new HashMap<>();

		while (iterator.hasNext()) {
			final Element record = iterator.next();

			count++;

			if ((count % 1000) == 0) {
				System.out.println("# Read records: " + count);
			}

			for (final Object o : record.selectNodes("//*[local-name() = 'collection']")) {
				final Element n = (Element) o;
				final String collName = n.getText().trim();
				if (StringUtils.isNotBlank(collName) && !collections.containsKey(collName)) {
					final Map<String, String> collAttrs = new HashMap<>();

					for (final Object ao : n.attributes()) {
						collAttrs.put(((Attribute) ao).getName(), ((Attribute) ao).getValue());
					}

					collections.put(collName, collAttrs);
				}
			}
		}

		collections.forEach((k, v) -> {
			System.out.println(k);
			v.forEach((ak, av) -> System.out.println(" - " + ak + "=" + av));
		});

		assertEquals(30000, count);
	}

}
