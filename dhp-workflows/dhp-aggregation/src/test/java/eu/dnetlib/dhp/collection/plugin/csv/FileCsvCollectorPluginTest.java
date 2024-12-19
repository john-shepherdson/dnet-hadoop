package eu.dnetlib.dhp.collection.plugin.csv;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.collection.CollectorException;

@Disabled
class FileCsvCollectorPluginTest {

	private FileCsvCollectorPlugin plugin;

	@Test
	void testCollect() throws CollectorException, DocumentException, IOException {

		this.plugin = new FileCsvCollectorPlugin(FileSystem.getLocal(new Configuration()));

		final ApiDescriptor api = new ApiDescriptor();
		api.setBaseUrl("file:///tmp/test.csv");
		api.setProtocol(CollectorPlugin.NAME.fileCSV.name());
		api.getParams().put("header", "true");
		api.getParams().put("identifier", "0");
		api.getParams().put("separator", ",");
		api.getParams().put("quote", "\"");

		final List<String> list = this.plugin.collect(api, null).toList();
		assertTrue(list.size() > 0);

		for (final String xml : list) {
			System.out.println(xml);
			assertTrue(StringUtils.isNotBlank(xml));
			assertTrue(StringUtils.isNotBlank((DocumentHelper.parseText(xml).valueOf("//*[@isId='true']"))));
		}
	}

}
