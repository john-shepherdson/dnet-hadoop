
package eu.dnetlib.dhp.collection.plugin.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(MockitoExtension.class)
public class FileGZipCollectorPluginTest {

	private static final Logger log = LoggerFactory.getLogger(FileGZipCollectorPluginTest.class);

	private final ApiDescriptor api = new ApiDescriptor();

	private FileGZipCollectorPlugin plugin;

	private static final String SPLIT_ON_ELEMENT = "repository";

	@BeforeEach
	public void setUp() throws IOException {

		final String gzipFile = Objects
			.requireNonNull(
				this
					.getClass()
					.getResource("/eu/dnetlib/dhp/collection/plugin/file/opendoar.xml.gz"))
			.getFile();

		api.setBaseUrl(gzipFile);

		HashMap<String, String> params = new HashMap<>();
		params.put("splitOnElement", SPLIT_ON_ELEMENT);

		api.setParams(params);

		FileSystem fs = FileSystem.get(new Configuration());
		plugin = new FileGZipCollectorPlugin(fs);
	}

	@Test
	void test() throws CollectorException {

		final Stream<String> stream = plugin.collect(api, new AggregatorReport());

		stream.limit(10).forEach(s -> {
			Assertions.assertTrue(s.length() > 0);
			log.info(s);
		});
	}
}
