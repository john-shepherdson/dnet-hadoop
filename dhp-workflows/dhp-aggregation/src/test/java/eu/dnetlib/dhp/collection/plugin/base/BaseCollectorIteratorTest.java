
package eu.dnetlib.dhp.collection.plugin.base;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.aggregation.AggregatorReport;

public class BaseCollectorIteratorTest {

	@Test
	void testImportFile() throws Exception {

		long count = 0;

		final BaseCollectorIterator iterator = new BaseCollectorIterator("base-sample.tar", new AggregatorReport());

		final Map<String, Map<String, String>> collections = new HashMap<>();
		final Map<String, AtomicInteger> fields = new HashMap<>();
		final Set<String> types = new HashSet<>();

		while (iterator.hasNext()) {

			final Document record = DocumentHelper.parseText(iterator.next());

			count++;

			if ((count % 1000) == 0) {
				System.out.println("# Read records: " + count);
			}

			// System.out.println(record.asXML());

			for (final Object o : record.selectNodes("//*|//@*")) {
				final String path = ((Node) o).getPath();

				if (fields.containsKey(path)) {
					fields.get(path).incrementAndGet();
				} else {
					fields.put(path, new AtomicInteger(1));
				}

				if (o instanceof Element) {
					final Element n = (Element) o;

					if ("collection".equals(n.getName())) {
						final String collName = n.getText().trim();
						if (StringUtils.isNotBlank(collName) && !collections.containsKey(collName)) {
							final Map<String, String> collAttrs = new HashMap<>();
							for (final Object ao : n.attributes()) {
								collAttrs.put(((Attribute) ao).getName(), ((Attribute) ao).getValue());
							}
							collections.put(collName, collAttrs);
						}
					} else if ("type".equals(n.getName())) {
						types.add(n.getText().trim());
					}

				}
			}

		}

		final ObjectMapper mapper = new ObjectMapper();
		for (final Entry<String, Map<String, String>> e : collections.entrySet()) {
			System.out.println(e.getKey() + ": " + mapper.writeValueAsString(e.getValue()));

		}

		for (final Entry<String, AtomicInteger> e : fields.entrySet()) {
			System.out.println(e.getKey() + ": " + e.getValue().get());

		}

		System.out.println("TYPES: ");
		for (final String s : types) {
			System.out.println(s);

		}

		assertEquals(30000, count);
	}

}
