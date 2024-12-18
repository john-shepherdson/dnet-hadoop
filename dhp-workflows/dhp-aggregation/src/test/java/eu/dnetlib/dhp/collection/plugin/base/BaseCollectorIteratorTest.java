
package eu.dnetlib.dhp.collection.plugin.base;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.aggregation.AggregatorReport;

@Disabled
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

	@Test
	public void testParquet() throws Exception {

		final String xml = IOUtils.toString(getClass().getResourceAsStream("record.xml"));

		final SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

		final List<BaseRecordInfo> ls = new ArrayList<>();

		for (int i = 0; i < 10; i++) {
			ls.add(extractInfo(xml));
		}

		final JavaRDD<BaseRecordInfo> rdd = JavaSparkContext
			.fromSparkContext(spark.sparkContext())
			.parallelize(ls);

		final Dataset<BaseRecordInfo> df = spark
			.createDataset(rdd.rdd(), Encoders.bean(BaseRecordInfo.class));

		df.printSchema();

		df.show(false);
	}

	private BaseRecordInfo extractInfo(final String s) {
		try {
			final Document record = DocumentHelper.parseText(s);

			final BaseRecordInfo info = new BaseRecordInfo();

			final Set<String> paths = new LinkedHashSet<>();
			final Set<String> types = new LinkedHashSet<>();
			final List<BaseCollectionInfo> colls = new ArrayList<>();

			for (final Object o : record.selectNodes("//*|//@*")) {
				paths.add(((Node) o).getPath());

				if (o instanceof Element) {
					final Element n = (Element) o;

					final String nodeName = n.getName();

					if ("collection".equals(nodeName)) {
						final String collName = n.getText().trim();

						if (StringUtils.isNotBlank(collName)) {
							final BaseCollectionInfo coll = new BaseCollectionInfo();
							coll.setId(collName);
							coll.setOpendoarId(n.valueOf("@opendoar_id").trim());
							coll.setRorId(n.valueOf("@ror_id").trim());
							colls.add(coll);
						}
					} else if ("type".equals(nodeName)) {
						types.add("TYPE: " + n.getText().trim());
					} else if ("typenorm".equals(nodeName)) {
						types.add("TYPE_NORM: " + n.getText().trim());
					}
				}
			}

			info.setId(record.valueOf("//*[local-name() = 'header']/*[local-name() = 'identifier']").trim());
			info.getTypes().addAll(types);
			info.getPaths().addAll(paths);
			info.setCollections(colls);

			return info;
		} catch (final DocumentException e) {
			throw new RuntimeException(e);
		}
	}

}
