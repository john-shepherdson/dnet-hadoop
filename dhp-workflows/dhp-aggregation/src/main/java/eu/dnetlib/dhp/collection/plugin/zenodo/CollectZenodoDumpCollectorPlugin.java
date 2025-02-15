
package eu.dnetlib.dhp.collection.plugin.zenodo;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class CollectZenodoDumpCollectorPlugin implements CollectorPlugin {

	final private Logger log = LoggerFactory.getLogger(getClass());

	private final  FileSystem fileSystem;

	public CollectZenodoDumpCollectorPlugin(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	private void downloadItem(final String name, final String itemURL, final String basePath,
		final FileSystem fileSystem) {
		try {
			final Path hdfsWritePath = new Path(String.format("%s/%s", basePath, name));
			final FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
			final HttpGet request = new HttpGet(itemURL);
			final int timeout = 60; // seconds
			final RequestConfig config = RequestConfig
				.custom()
				.setConnectTimeout(timeout * 1000)
				.setConnectionRequestTimeout(timeout * 1000)
				.setSocketTimeout(timeout * 1000)
				.build();
			log.info("Downloading url {} into {}", itemURL, hdfsWritePath.getName());
			try (CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
				CloseableHttpResponse response = client.execute(request)) {
				int responseCode = response.getStatusLine().getStatusCode();
				log.info("Response code is {}", responseCode);
				if (responseCode >= 200 && responseCode < 400) {
					IOUtils.copy(response.getEntity().getContent(), fsDataOutputStream);
					fsDataOutputStream.flush();
					fsDataOutputStream.hflush();
					fsDataOutputStream.close();
				}
			} catch (Throwable eu) {
				throw new RuntimeException(eu);
			}
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}



	@Override
	public Stream<String> collect(ApiDescriptor api, AggregatorReport report) throws CollectorException {

		final String zenodoURL = api.getBaseUrl();
		return doStream(zenodoURL, "/tmp");
	}

	public Stream<String> doStream( String zenodoURL, String basePath) throws CollectorException {
		try {

			downloadItem("zenodoDump.tar.gz", zenodoURL, basePath, fileSystem);
			CompressionCodecFactory factory = new CompressionCodecFactory(fileSystem.getConf());
			Path sourcePath = new Path(basePath + "/zenodoDump.tar.gz");
			CompressionCodec codec = factory.getCodec(sourcePath);
			InputStream gzipInputStream = null;
			try {
				gzipInputStream = codec.createInputStream(fileSystem.open(sourcePath));
				return iterateTar(gzipInputStream);

			} catch (IOException e) {
				throw new CollectorException(e);
			}
		} catch (Exception e) {
			throw new CollectorException(e);
		}
	}

	private Stream<String> iterateTar(InputStream gzipInputStream) throws Exception {

		Iterable<String> iterable = () -> new ZenodoTarIterator(gzipInputStream);
		return StreamSupport.stream(iterable.spliterator(), false);

	}
}
