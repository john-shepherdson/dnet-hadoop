
package eu.dnetlib.dhp.collection.orcid;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class DownloadORCIDDumpApplication {
	private static final Logger log = LoggerFactory.getLogger(DownloadORCIDDumpApplication.class);

	private final FileSystem fileSystem;

	public DownloadORCIDDumpApplication(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							DownloadORCIDDumpApplication.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/collection/orcid/download_orcid_parameter.json"))));
		argumentParser.parseArgument(args);

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI is {}", hdfsuri);

		final String targetPath = argumentParser.get("targetPath");
		log.info("targetPath is {}", targetPath);

		final String apiURL = argumentParser.get("apiURL");
		log.info("apiURL is {}", apiURL);

		final FileSystem fileSystem = FileSystem.get(getHadoopConfiguration(hdfsuri));

		new DownloadORCIDDumpApplication(fileSystem).run(targetPath, apiURL);

	}

	private void downloadItem(final String name, final String itemURL, final String basePath) {
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
				}
			} catch (Throwable eu) {
				throw new RuntimeException(eu);
			}
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	protected void run(final String targetPath, final String apiURL) throws Exception {
		final ObjectMapper mapper = new ObjectMapper();
		final URL url = new URL(apiURL);
		URLConnection conn = url.openConnection();
		InputStream is = conn.getInputStream();
		final String json = IOUtils.toString(is);
		JsonNode jsonNode = mapper.readTree(json);
		jsonNode
			.get("files")
			.forEach(i -> downloadItem(i.get("name").asText(), i.get("download_url").asText(), targetPath));
	}
}
