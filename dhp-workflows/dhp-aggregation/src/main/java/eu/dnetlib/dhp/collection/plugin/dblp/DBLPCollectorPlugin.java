
package eu.dnetlib.dhp.collection.plugin.dblp;

import java.io.InputStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class DBLPCollectorPlugin implements CollectorPlugin {

	private final FileSystem fileSystem;

	public DBLPCollectorPlugin(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {
		return doStream(api.getBaseUrl());
	}

	private Stream<String> doStream(String dblpURL) throws CollectorException {
		try {
			CompressionCodecFactory factory = new CompressionCodecFactory(fileSystem.getConf());
			Path sourcePath = new Path(dblpURL);
			CompressionCodec codec = factory.getCodec(sourcePath);
			InputStream gis = codec.createInputStream(fileSystem.open(sourcePath));
			Iterable<String> iterable = () -> {
				try {
					return new DBLPParser(gis);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			};
			return StreamSupport.stream(iterable.spliterator(), false);
		} catch (Throwable e) {
			throw new CollectorException(e);
		}
	}

}
