
package eu.dnetlib.dhp.common.api;

import java.io.*;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.jetbrains.annotations.NotNull;

import com.google.gson.Gson;

import eu.dnetlib.dhp.common.api.zenodo.ZenodoModel;
import eu.dnetlib.dhp.common.api.zenodo.ZenodoModelList;
import okhttp3.*;

public class ZenodoAPIClient implements Serializable {

	String urlString;
	String bucket;

	String deposition_id;
	String access_token;

	public static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");

	private static final MediaType MEDIA_TYPE_ZIP = MediaType.parse("application/zip");

	public String getUrlString() {
		return urlString;
	}

	public void setUrlString(String urlString) {
		this.urlString = urlString;
	}

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public void setDeposition_id(String deposition_id) {
		this.deposition_id = deposition_id;
	}

	public ZenodoAPIClient(String urlString, String access_token) {

		this.urlString = urlString;
		this.access_token = access_token;
	}

	/**
	 * Brand new deposition in Zenodo. It sets the deposition_id and the bucket where to store the files to upload
	 *
	 * @return response code
	 * @throws IOException
	 */
	public int newDeposition() throws IOException {
		String json = "{}";

		URL url = new URL(urlString);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + access_token);
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);
		try (OutputStream os = conn.getOutputStream()) {
			byte[] input = json.getBytes("utf-8");
			os.write(input, 0, input.length);
		}

		String body = getBody(conn);

		int responseCode = conn.getResponseCode();
		conn.disconnect();

		if (!checkOKStatus(responseCode))
			throw new IOException("Unexpected code " + responseCode + body);

		ZenodoModel newSubmission = new Gson().fromJson(body, ZenodoModel.class);
		this.bucket = newSubmission.getLinks().getBucket();
		this.deposition_id = newSubmission.getId();

		return responseCode;
	}

	/**
	 * Upload files in Zenodo.
	 *
	 * @param is the inputStream for the file to upload
	 * @param file_name the name of the file as it will appear on Zenodo
	 * @return the response code
	 */
	public int uploadIS(InputStream is, String file_name) throws IOException {

		URL url = new URL(bucket + "/" + file_name);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "application/zip");
		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + access_token);
		conn.setDoOutput(true);
		conn.setRequestMethod("PUT");

		byte[] buf = new byte[8192];
		int length;
		try (OutputStream os = conn.getOutputStream()) {
			while ((length = is.read(buf)) != -1) {
				os.write(buf, 0, length);
			}

		}
		int responseCode = conn.getResponseCode();
		if (!checkOKStatus(responseCode)) {
			throw new IOException("Unexpected code " + responseCode + getBody(conn));
		}

		return responseCode;
	}

	@NotNull
	private String getBody(HttpURLConnection conn) throws IOException {
		String body = "{}";
		try (BufferedReader br = new BufferedReader(
			new InputStreamReader(conn.getInputStream(), "utf-8"))) {
			StringBuilder response = new StringBuilder();
			String responseLine = null;
			while ((responseLine = br.readLine()) != null) {
				response.append(responseLine.trim());
			}

			body = response.toString();

		}
		return body;
	}

	/**
	 * Associates metadata information to the current deposition
	 *
	 * @param metadata the metadata
	 * @return response code
	 * @throws IOException
	 */
	public int sendMretadata(String metadata) throws IOException {

		URL url = new URL(urlString + "/" + deposition_id);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + access_token);
		conn.setDoOutput(true);
		conn.setRequestMethod("PUT");

		try (OutputStream os = conn.getOutputStream()) {
			byte[] input = metadata.getBytes("utf-8");
			os.write(input, 0, input.length);

		}

		final int responseCode = conn.getResponseCode();
		conn.disconnect();
		if (!checkOKStatus(responseCode))
			throw new IOException("Unexpected code " + responseCode + getBody(conn));

		return responseCode;

	}

	private boolean checkOKStatus(int responseCode) {

		if (HttpURLConnection.HTTP_OK != responseCode ||
			HttpURLConnection.HTTP_CREATED != responseCode)
			return true;
		return false;
	}

	/**
	 * To publish the current deposition. It works for both new deposition or new version of an old deposition
	 *
	 * @return response code
	 * @throws IOException
	 */
	@Deprecated
	public int publish() throws IOException {

		String json = "{}";

		OkHttpClient httpClient = new OkHttpClient.Builder().connectTimeout(600, TimeUnit.SECONDS).build();

		RequestBody body = RequestBody.create(json, MEDIA_TYPE_JSON);

		Request request = new Request.Builder()
			.url(urlString + "/" + deposition_id + "/actions/publish")
			.addHeader("Authorization", "Bearer " + access_token)
			.post(body)
			.build();

		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful())
				throw new IOException("Unexpected code " + response + response.body().string());

			return response.code();

		}
	}

	/**
	 * To create a new version of an already published deposition. It sets the deposition_id and the bucket to be used
	 * for the new version.
	 *
	 * @param concept_rec_id the concept record id of the deposition for which to create a new version. It is the last
	 *            part of the url for the DOI Zenodo suggests to use to cite all versions: DOI: 10.xxx/zenodo.656930
	 *            concept_rec_id = 656930
	 * @return response code
	 */
	public int newVersion(String concept_rec_id) throws IOException, MissingConceptDoiException {
		setDepositionId(concept_rec_id, 1);
		String json = "{}";

		URL url = new URL(urlString + "/" + deposition_id + "/actions/newversion");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();

		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + access_token);
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");

		try (OutputStream os = conn.getOutputStream()) {
			byte[] input = json.getBytes("utf-8");
			os.write(input, 0, input.length);

		}

		String body = getBody(conn);

		int responseCode = conn.getResponseCode();

		conn.disconnect();
		if (!checkOKStatus(responseCode))
			throw new IOException("Unexpected code " + responseCode + body);

		ZenodoModel zenodoModel = new Gson().fromJson(body, ZenodoModel.class);
		String latest_draft = zenodoModel.getLinks().getLatest_draft();
		deposition_id = latest_draft.substring(latest_draft.lastIndexOf("/") + 1);
		bucket = getBucket(latest_draft);

		return responseCode;

	}

	/**
	 * To finish uploading a version or new deposition not published
	 * It sets the deposition_id and the bucket to be used
	 *
	 *
	 * @param deposition_id the deposition id of the not yet published upload
	 *            concept_rec_id = 656930
	 * @return response code
	 * @throws IOException
	 * @throws MissingConceptDoiException
	 */
	public int uploadOpenDeposition(String deposition_id) throws IOException, MissingConceptDoiException {

		this.deposition_id = deposition_id;

		String json = "{}";

		URL url = new URL(urlString + "/" + deposition_id);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();

		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + access_token);
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);
		try (OutputStream os = conn.getOutputStream()) {
			byte[] input = json.getBytes("utf-8");
			os.write(input, 0, input.length);
		}

		String body = getBody(conn);

		int responseCode = conn.getResponseCode();
		conn.disconnect();

		if (!checkOKStatus(responseCode))
			throw new IOException("Unexpected code " + responseCode + body);

		ZenodoModel zenodoModel = new Gson().fromJson(body, ZenodoModel.class);
		bucket = zenodoModel.getLinks().getBucket();

		return responseCode;

	}

	private void setDepositionId(String concept_rec_id, Integer page) throws IOException, MissingConceptDoiException {

		ZenodoModelList zenodoModelList = new Gson()
			.fromJson(getPrevDepositions(String.valueOf(page)), ZenodoModelList.class);

		for (ZenodoModel zm : zenodoModelList) {
			if (zm.getConceptrecid().equals(concept_rec_id)) {
				deposition_id = zm.getId();
				return;
			}
		}
		if (zenodoModelList.size() == 0)
			throw new MissingConceptDoiException(
				"The concept record id specified was missing in the list of depositions");
		setDepositionId(concept_rec_id, page + 1);

	}

	private String getPrevDepositions(String page) throws IOException {

		HttpUrl.Builder urlBuilder = HttpUrl.parse(urlString).newBuilder();
		urlBuilder.addQueryParameter("page", page);

		URL url = new URL(urlBuilder.build().toString());
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + access_token);
		conn.setDoOutput(true);
		conn.setRequestMethod("GET");

		String body = getBody(conn);

		int responseCode = conn.getResponseCode();

		conn.disconnect();
		if (!checkOKStatus(responseCode))
			throw new IOException("Unexpected code " + responseCode + body);

		return body;

	}

	private String getBucket(String inputUurl) throws IOException {

		URL url = new URL(inputUurl);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
		conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + access_token);
		conn.setDoOutput(true);
		conn.setRequestMethod("GET");

		String body = getBody(conn);

		int responseCode = conn.getResponseCode();

		conn.disconnect();
		if (!checkOKStatus(responseCode))
			throw new IOException("Unexpected code " + responseCode + body);

		ZenodoModel zenodoModel = new Gson().fromJson(body, ZenodoModel.class);

		return zenodoModel.getLinks().getBucket();

	}

}
