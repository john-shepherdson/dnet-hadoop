
package eu.dnetlib.dhp.collection.plugin.mongodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.dhp.aggregation.common.AggregatorReport;
import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.CollectorException;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.utils.DHPUtils;

public class MongoDbDumpCollectorPlugin implements CollectorPlugin {

	public static final String PATH_PARAM = "path";
	public static final String BODY_JSONPATH = "$.body";

	public FileSystem fileSystem;

	public MongoDbDumpCollectorPlugin(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {

		final Path path = Optional
			.ofNullable(api.getParams().get("path"))
			.map(Path::new)
			.orElseThrow(() -> new CollectorException(String.format("missing parameter '%s'", PATH_PARAM)));

		try {
			if (!fileSystem.exists(path)) {
				throw new CollectorException("path does not exist: " + path.toString());
			}

			return new BufferedReader(
				new InputStreamReader(new GZIPInputStream(fileSystem.open(path)), Charset.defaultCharset()))
					.lines()
					.map(s -> DHPUtils.getJPathString(BODY_JSONPATH, s));

		} catch (IOException e) {
			throw new CollectorException(e);
		}
	}
}
