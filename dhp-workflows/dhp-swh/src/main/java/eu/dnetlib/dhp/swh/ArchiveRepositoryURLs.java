
package eu.dnetlib.dhp.swh;

import static eu.dnetlib.dhp.utils.DHPUtils.getHadoopConfiguration;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions;
import eu.dnetlib.dhp.swh.models.LastVisitData;
import eu.dnetlib.dhp.swh.utils.SWHConnection;
import eu.dnetlib.dhp.swh.utils.SWHConstants;
import eu.dnetlib.dhp.swh.utils.SWHUtils;

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

		final String apiAccessToken = argumentParser.get("apiAccessToken");
		log.info("apiAccessToken: {}", apiAccessToken);

		final HttpClientParams clientParams = SWHUtils.getClientParams(argumentParser);

		swhConnection = new SWHConnection(clientParams, apiAccessToken);

		final FileSystem fs = FileSystem.get(getHadoopConfiguration(hdfsuri));

		archive(fs, inputPath, outputPath, archiveThresholdInDays);

	}

	private static void archive(FileSystem fs, String inputPath, String outputPath, Integer archiveThresholdInDays)
		throws IOException {

		SequenceFile.Reader fr = SWHUtils.getSequenceFileReader(fs, inputPath);
		SequenceFile.Writer fw = SWHUtils.getSequenceFileWriter(fs, outputPath);

		// Create key and value objects to hold data
		Text repoUrl = new Text();
		Text lastVisitData = new Text();

		// Read key-value pairs from the SequenceFile and handle appropriately
		while (fr.next(repoUrl, lastVisitData)) {

			String response = null;
			try {
				response = handleRecord(repoUrl.toString(), lastVisitData.toString(), archiveThresholdInDays);
			} catch (java.text.ParseException e) {
				log.error("Could not handle record with repo Url: {}", repoUrl.toString());
				throw new RuntimeException(e);
			}

			// response is equal to null when no need for request
			if (response != null) {
				SWHUtils.appendToSequenceFile(fw, repoUrl.toString(), response);
			}

		}

		// Close readers
		fw.close();
		fr.close();
	}

	public static String handleRecord(String repoUrl, String lastVisitData, Integer archiveThresholdInDays)
		throws IOException, java.text.ParseException {

		log.info("{ Key: {}, Value: {} }", repoUrl, lastVisitData);

		LastVisitData lastVisit = OBJECT_MAPPER.readValue(lastVisitData, LastVisitData.class);

		// a previous attempt for archival has been made, and repository URL was not found
		// avoid performing the same archive request again
		if (lastVisit.getStatus() != null &&
			lastVisit.getStatus().equals(SWHConstants.VISIT_STATUS_NOT_FOUND)) {

			log.info("Avoid request -- previous archive request returned NOT_FOUND");
			return null;
		}

		// if we have last visit data
		if (lastVisit.getSnapshot() != null) {

			String cleanDate = GraphCleaningFunctions.cleanDate(lastVisit.getDate());

			// and the last visit date can be parsed
			if (cleanDate != null) {

				SimpleDateFormat formatter = new SimpleDateFormat(ModelSupport.DATE_FORMAT);
				Date lastVisitDate = formatter.parse(cleanDate);

				// OR last visit time < (now() - archiveThresholdInDays)
				long diffInMillies = Math.abs((new Date()).getTime() - lastVisitDate.getTime());
				long diffInDays = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
				log.info("Date diff from now (in days): {}", diffInDays);

				// do not perform a request, if the last visit date is no older than $archiveThresholdInDays
				if (archiveThresholdInDays >= diffInDays) {
					log.info("Avoid request -- no older than {} days", archiveThresholdInDays);
					return null;
				}
			}
		}

		// ELSE perform an archive request
		log.info("Perform archive request for: {}", repoUrl);

		// if last visit data are available, re-use version control type,
		// else use the default one (i.e., git)
		String visitType = Optional
			.ofNullable(lastVisit.getType())
			.orElse(SWHConstants.DEFAULT_VISIT_TYPE);

		URL url = new URL(String.format(SWHConstants.SWH_ARCHIVE_URL, visitType, repoUrl.trim()));

		log.info("Sending archive request: {}", url);

		String response;
		try {
			response = swhConnection.call(url.toString());
		} catch (CollectorException e) {
			log.error("Error in request: {}", url);
			response = "{}";
		}

		return response;
	}

}
