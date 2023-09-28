
package eu.dnetlib.dhp.swh;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.swh.utils.SWHConnection;
import eu.dnetlib.dhp.swh.utils.SWHConstants;
import eu.dnetlib.dhp.swh.utils.SWHUtils;

/**
 * Given a file with software repository URLs, this class
 * collects last visit data from the Software Heritage API.
 *
 * @author Serafeim Chatzopoulos
 */
public class CollectLastVisitRepositoryData {

	private static final Logger log = LoggerFactory.getLogger(CollectLastVisitRepositoryData.class);
	private static SWHConnection swhConnection = null;

	public static void main(final String[] args)
		throws IOException, ParseException, InterruptedException, URISyntaxException, CollectorException {
		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					CollectLastVisitRepositoryData.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/swh/input_collect_last_visit_repository_data.json")));
		argumentParser.parseArgument(args);

		log.info("Java Xmx: {}m", Runtime.getRuntime().maxMemory() / (1024 * 1024));

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI: {}", hdfsuri);

		final String inputPath = argumentParser.get("softwareCodeRepositoryURLs");
		log.info("inputPath: {}", inputPath);

		final String outputPath = argumentParser.get("lastVisitsPath");
		log.info("outputPath: {}", outputPath);

		final HttpClientParams clientParams = SWHUtils.getClientParams(argumentParser);

		swhConnection = new SWHConnection(clientParams);

		final FileSystem fs = FileSystem.get(getHadoopConfiguration(hdfsuri));

		collect(fs, inputPath, outputPath);

		fs.close();
	}

	private static void collect(FileSystem fs, String inputPath, String outputPath)
		throws IOException {

		SequenceFile.Writer fw = SWHUtils.getSequenceFileWriter(fs, outputPath);

		// Specify the HDFS directory path you want to read
		Path directoryPath = new Path(inputPath);

		// List all files in the directory
		FileStatus[] partStatuses = fs.listStatus(directoryPath);

		for (FileStatus partStatus : partStatuses) {

			// Check if it's a file (not a directory)
			if (partStatus.isFile()) {
				handleFile(fs, partStatus.getPath(), fw);
			}

		}

		fw.close();
	}

	private static void handleFile(FileSystem fs, Path partInputPath, SequenceFile.Writer fw)
		throws IOException {

		BufferedReader br = SWHUtils.getFileReader(fs, partInputPath);

		String repoUrl;
		while ((repoUrl = br.readLine()) != null) {

			URL url = new URL(String.format(SWHConstants.SWH_LATEST_VISIT_URL, repoUrl.trim()));

			String response;
			try {
				response = swhConnection.call(url.toString());
			} catch (CollectorException e) {
				log.error("Error in request: {}", url);
				response = "{}";
			}

			SWHUtils.appendToSequenceFile(fw, repoUrl, response);
		}

		br.close();
	}

}
