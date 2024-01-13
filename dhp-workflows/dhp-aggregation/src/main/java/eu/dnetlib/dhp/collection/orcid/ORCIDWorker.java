
package eu.dnetlib.dhp.collection.orcid;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

import javax.swing.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.http.HttpHeaders;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.collection.HttpClientParams;

public class ORCIDWorker extends Thread {

	final static Logger log = LoggerFactory.getLogger(ORCIDWorker.class);

	public static String JOB_COMPLETE = "JOB_COMPLETE";

	private static final String userAgent = "Mozilla/5.0 (compatible; OAI; +http://www.openaire.eu)";

	private final BlockingQueue<String> queue;

	private boolean hasComplete = false;

	private final SequenceFile.Writer employments;

	private final SequenceFile.Writer summary;
	private final SequenceFile.Writer works;

	private final String token;

	private final String id;

	public static ORCIDWorkerBuilder builder() {
		return new ORCIDWorkerBuilder();
	}

	public ORCIDWorker(String id, BlockingQueue<String> myqueue, SequenceFile.Writer employments,
		SequenceFile.Writer summary, SequenceFile.Writer works, String token) {
		this.id = id;
		this.queue = myqueue;
		this.employments = employments;
		this.summary = summary;
		this.works = works;
		this.token = token;
	}

	public static String retrieveURL(final String id, final String apiUrl, String token) {
		try {
			final HttpURLConnection urlConn = getHttpURLConnection(apiUrl, token);
			if (urlConn.getResponseCode() > 199 && urlConn.getResponseCode() < 300) {
				InputStream input = urlConn.getInputStream();
				return IOUtils.toString(input);
			} else {
				log
					.error(
						"Thread {} UNABLE TO DOWNLOAD FROM THIS URL {} , status code {}", id, apiUrl,
						urlConn.getResponseCode());
			}
		} catch (Exception e) {
			log.error("Thread {}  Error on retrieving URL {} {}", id, apiUrl, e);
		}
		return null;
	}

	@NotNull
	private static HttpURLConnection getHttpURLConnection(String apiUrl, String token) throws IOException {
		final HttpURLConnection urlConn = (HttpURLConnection) new URL(apiUrl).openConnection();
		final HttpClientParams clientParams = new HttpClientParams();
		urlConn.setInstanceFollowRedirects(false);
		urlConn.setReadTimeout(clientParams.getReadTimeOut() * 1000);
		urlConn.setConnectTimeout(clientParams.getConnectTimeOut() * 1000);
		urlConn.addRequestProperty(HttpHeaders.USER_AGENT, userAgent);
		urlConn.addRequestProperty(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", token));
		return urlConn;
	}

	private static String generateSummaryURL(final String orcidId) {
		return "https://api.orcid.org/v3.0/" + orcidId + "/record";
	}

	private static String generateWorksURL(final String orcidId) {
		return "https://api.orcid.org/v3.0/" + orcidId + "/works";
	}

	private static String generateEmploymentsURL(final String orcidId) {
		return "https://api.orcid.org/v3.0/" + orcidId + "/employments";
	}

	private static void writeResultToSequenceFile(String id, String url, String token, String orcidId,
		SequenceFile.Writer file) throws IOException {
		final String response = retrieveURL(id, url, token);
		if (response != null) {
			if (orcidId == null) {
				log.error("Thread {}   {}   {}", id, orcidId, response);
				throw new RuntimeException("null items ");
			}

			if (file == null) {
				log.error("Thread {}   file is null for {}  URL:{}", id, url, orcidId);
			} else {
				file.append(new Text(orcidId), new Text(response));
				file.hflush();
			}

		}

	}

	@Override
	public void run() {
		final Text key = new Text();
		final Text value = new Text();
		long start;
		long total_time;
		String orcidId = "";
		int requests = 0;
		if (summary == null || employments == null || works == null)
			throw new RuntimeException("Null files");

		while (!hasComplete) {
			try {

				orcidId = queue.take();

				if (orcidId.equalsIgnoreCase(JOB_COMPLETE)) {
					queue.put(orcidId);
					hasComplete = true;
				} else {
					start = System.currentTimeMillis();
					writeResultToSequenceFile(id, generateSummaryURL(orcidId), token, orcidId, summary);
					total_time = System.currentTimeMillis() - start;
					requests++;
					if (total_time < 1000) {
						// I know making a sleep on a thread is bad, but we need to stay to 24 requests per seconds,
						// hence
						// the time between two http request in a thread must be 1 second
						Thread.sleep(1000L - total_time);
					}
					start = System.currentTimeMillis();
					writeResultToSequenceFile(id, generateWorksURL(orcidId), token, orcidId, works);
					total_time = System.currentTimeMillis() - start;
					requests++;
					if (total_time < 1000) {
						// I know making a sleep on a thread is bad, but we need to stay to 24 requests per seconds,
						// hence
						// the time between two http request in a thread must be 1 second
						Thread.sleep(1000L - total_time);
					}
					start = System.currentTimeMillis();
					writeResultToSequenceFile(id, generateEmploymentsURL(orcidId), token, orcidId, employments);
					total_time = System.currentTimeMillis() - start;
					requests++;
					if (total_time < 1000) {
						// I know making a sleep on a thread is bad, but we need to stay to 24 requests per seconds,
						// hence
						// the time between two http request in a thread must be 1 second
						Thread.sleep(1000L - total_time);
					}
					if (requests % 30 == 0) {
						log.info("Thread {}   Downloaded {}", id, requests);
					}
				}

			} catch (Throwable e) {

				log.error("Thread {}  Unable to save ORICD: {} item error", id, orcidId, e);

			}

		}
		try {
			works.close();
			summary.close();
			employments.close();
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}

		log.info("Thread {}  COMPLETE ", id);
		log.info("Thread {}   Downloaded {}", id, requests);

	}

	public static class ORCIDWorkerBuilder {

		private String id;
		private SequenceFile.Writer employments;
		private SequenceFile.Writer summary;
		private SequenceFile.Writer works;
		private BlockingQueue<String> queue;

		private String token;

		public ORCIDWorkerBuilder withId(final String id) {
			this.id = id;
			return this;
		}

		public ORCIDWorkerBuilder withEmployments(final SequenceFile.Writer sequenceFile) {
			this.employments = sequenceFile;
			return this;
		}

		public ORCIDWorkerBuilder withSummary(final SequenceFile.Writer sequenceFile) {
			this.summary = sequenceFile;
			return this;
		}

		public ORCIDWorkerBuilder withWorks(final SequenceFile.Writer sequenceFile) {
			this.works = sequenceFile;
			return this;
		}

		public ORCIDWorkerBuilder withAccessToken(final String accessToken) {
			this.token = accessToken;
			return this;
		}

		public ORCIDWorkerBuilder withBlockingQueue(final BlockingQueue<String> queue) {
			this.queue = queue;
			return this;
		}

		public ORCIDWorker build() {
			if (this.summary == null || this.works == null || this.employments == null || StringUtils.isEmpty(token)
				|| queue == null)
				throw new RuntimeException("Unable to build missing required params");
			return new ORCIDWorker(id, queue, employments, summary, works, token);
		}

	}

}
