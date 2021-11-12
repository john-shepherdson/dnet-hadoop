
package eu.dnetlib.dhp.oa.dedup;

import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class UpdateOpenorgsJob {

	private static final Logger log = LoggerFactory.getLogger(UpdateOpenorgsJob.class);

	public static void main(String[] args) throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/updateOpenorgsJob_parameters.json")));
		parser.parseArgument(args);

		final String apiUrl = parser.get("apiUrl");
		final int delay = Integer.parseInt(parser.get("delay"));

		log.info("apiUrl: '{}'", apiUrl);
		log.info("delay: '{}'", delay);

		APIResponse res = httpCall(apiUrl);
		while (res != null && res.getStatus().equals(ImportStatus.RUNNING)) {
			TimeUnit.MINUTES.sleep(delay);
			res = httpCall(apiUrl + "/status");
		}

		if (res == null) {
			log.error("Openorgs Update FAILED: No response");
			throw new RuntimeException("Openorgs Update FAILED: No response");
		}

		if (res.getStatus() == null || !res.getStatus().equals(ImportStatus.SUCCESS)) {
			log.error("Openorgs Update FAILED: '{}' - '{}'", res.getStatus(), res.getMessage());
			throw new RuntimeException(res.getMessage());
		}

	}

	private static APIResponse httpCall(final String url) throws Exception {
		final HttpGet req = new HttpGet(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				final String s = IOUtils.toString(response.getEntity().getContent());
				return (new ObjectMapper()).readValue(s, APIResponse.class);
			}
		}
	}

}

class APIResponse {
	private String id;
	private Long dateStart;
	private Long dateEnd;
	private ImportStatus status;
	private String message;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getDateStart() {
		return dateStart;
	}

	public void setDateStart(Long dateStart) {
		this.dateStart = dateStart;
	}

	public Long getDateEnd() {
		return dateEnd;
	}

	public void setDateEnd(Long dateEnd) {
		this.dateEnd = dateEnd;
	}

	public ImportStatus getStatus() {
		return status;
	}

	public void setStatus(ImportStatus status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
}

enum ImportStatus {
	SUCCESS, FAILED, RUNNING, NOT_LAUNCHED, NOT_YET_STARTED
}
