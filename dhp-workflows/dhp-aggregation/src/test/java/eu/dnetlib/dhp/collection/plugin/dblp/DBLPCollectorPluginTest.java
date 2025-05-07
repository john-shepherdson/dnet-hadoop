
package eu.dnetlib.dhp.collection.plugin.dblp;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.collection.ApiDescriptor;

class DBLPCollectorPluginTest {

	private DBLPCollectorPlugin plugin;

	private ApiDescriptor api;

	private static final String baseURL = "file:///Users/michele/Downloads/dblp.xml.gz";

	@BeforeEach
	void setUp() throws Exception {
		final FileSystem fs = FileSystem.getLocal(new Configuration());

		this.plugin = new DBLPCollectorPlugin(fs);

		this.api = new ApiDescriptor();
		this.api.setBaseUrl(baseURL);
		this.api.setProtocol("dblp");
	}

	@Test
	@Disabled
	public void testOne() throws Exception {
		System.out.println("one publication");

		final Iterator<String> iterator = this.plugin.collect(this.api, null).iterator();

		if (iterator.hasNext()) {
			final String res = iterator.next();
			assertNotNull(res);
			System.out.println(res);
		}
	}

	@Test
	@Disabled
	public void testCompleteHarvesting() throws Exception {
		System.out.println("testing complete harvesting");
		final Iterator<String> iterator = this.plugin.collect(this.api, null).iterator();
		int i = 0;
		while (iterator.hasNext()) {
			final String res = iterator.next();
			assertNotNull(res);
			// System.out.println(res);
			i++;

		}
		System.out.println("Got " + i + " publications");
		assertTrue(i > 0);
	}

	@Test
	@Disabled
	public void testExtractXPaths() throws Exception {
		System.out.println("testExtractXPaths");

		final Set<String> set = new LinkedHashSet<>();
		this.plugin.collect(this.api, null).forEach(s -> {

			try {
				final SAXReader reader = new SAXReader();
				// reader.setEntityResolver((publicId, systemId) -> new
				// InputSource(getClass().getResourceAsStream("dblp.dtd")));

				final Document doc = reader.read(new StringReader(s));

				for (final Object o : doc.selectNodes("//*|//@*")) {
					set.add(((Node) o).getPath());
				}
			} catch (final DocumentException e) {
				e.printStackTrace();
			}
		});

		for (final String s : set) {
			System.out.println("XPATH: " + s);
		}
	}

}
