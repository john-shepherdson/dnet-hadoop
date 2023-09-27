
package eu.dnetlib.dhp.swh;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.swh.models.LastVisitData;
import eu.dnetlib.dhp.swh.utils.SWHConnection;
import eu.dnetlib.dhp.swh.utils.SWHConstants;
import eu.dnetlib.dhp.swh.utils.SWHUtils;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static eu.dnetlib.dhp.common.Constants.REQUEST_METHOD;
import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

/**
 * Sends archive requests to the SWH API for those software repository URLs that are missing from them
 *
 *  @author Serafeim Chatzopoulos
 */
public class ArchiveRepositoryURLs {

	private static final Logger log = LoggerFactory.getLogger(ArchiveRepositoryURLs.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SWHConnection swhConnection = null;

	public static void main(final String[] args) throws IOException, ParseException {
		final ArgumentApplicationParser argumentParser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					CollectLastVisitRepositoryData.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/swh/input_archive_repository_urls.json")));
		argumentParser.parseArgument(args);

		final String hdfsuri = argumentParser.get("namenode");
		log.info("hdfsURI: {}", hdfsuri);

		final String inputPath = argumentParser.get("lastVisitsPath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = argumentParser.get("archiveRequestsPath");
		log.info("outputPath: {}", outputPath);

		final Integer archiveThresholdInDays = Integer.parseInt(argumentParser.get("archiveThresholdInDays"));
		log.info("archiveThresholdInDays: {}", archiveThresholdInDays);

		final HttpClientParams clientParams = SWHUtils.getClientParams(argumentParser);

		swhConnection = new SWHConnection(clientParams);

		final FileSystem fs = FileSystem.get(getHadoopConfiguration(hdfsuri));

		archive(fs, inputPath, outputPath, archiveThresholdInDays);

	}

	private static void archive(FileSystem fs, String inputPath, String outputPath, Integer archiveThresholdInDays) throws IOException {

		SequenceFile.Reader fr = SWHUtils.getSequenceFileReader(fs, inputPath);
		SequenceFile.Writer fw = SWHUtils.getSequenceFileWriter(fs, outputPath);

		// Create key and value objects to hold data
		Text repoUrl = new Text();
		Text lastVisitData = new Text();

		// Read key-value pairs from the SequenceFile and handle appropriately
		while (fr.next(repoUrl, lastVisitData)) {

			String response = handleRecord(repoUrl.toString(), lastVisitData.toString(), archiveThresholdInDays);

			// response is equal to null when no need for request
			if (response != null) {
				SWHUtils.appendToSequenceFile(fw, repoUrl.toString(), response);
			}

		}

		// Close readers
		fw.close();
		fr.close();
	}

	public static String handleRecord(String repoUrl, String lastVisitData, Integer archiveThresholdInDays) throws IOException {
		System.out.println("Key: " + repoUrl + ", Value: " + lastVisitData);

		LastVisitData lastVisit = OBJECT_MAPPER.readValue(lastVisitData, LastVisitData.class);

		// perform an archive request when no repoUrl was not found in previous step
		if (lastVisit.getSnapshot() != null) {

			// OR last visit was before (now() - archiveThresholdInDays)
			long diffInMillies = Math.abs((new Date()).getTime() - lastVisit.getDate().getTime());
			long diffInDays = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);

			if (archiveThresholdInDays >= diffInDays) {
				return null;
			}
		}

		// if last visit data are available, re-use version control type, else use the default one (i.e., git)
		String visitType = Optional
			.ofNullable(lastVisit.getType())
			.orElse(SWHConstants.DEFAULT_VISIT_TYPE);

		URL url = new URL(String.format(SWHConstants.SWH_ARCHIVE_URL, visitType, repoUrl.trim()));
		System.out.println(url.toString());

		String response;
		try {
			response = swhConnection.call(url.toString());
		} catch (CollectorException e) {
			log.info("Error in request: {}", url);
			response = "{}";
		}

		return response;

	}



}
