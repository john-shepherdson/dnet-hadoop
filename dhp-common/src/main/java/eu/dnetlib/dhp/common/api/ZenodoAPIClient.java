
package eu.dnetlib.dhp.common.api;

import java.io.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

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

	public ZenodoAPIClient(String urlString, String access_token) throws IOException {

		this.urlString = urlString;
		this.access_token = access_token;
	}

	/**
	 * Brand new deposition in Zenodo. It sets the deposition_id and the bucket where to store the files to upload
	 * @return response code
	 * @throws IOException
	 */
	public int newDeposition() throws IOException {
		String json = "{}";
		OkHttpClient httpClient = new OkHttpClient();

		RequestBody body = RequestBody.create(MEDIA_TYPE_JSON, json);

		Request request = new Request.Builder()
			.url(urlString)
			.addHeader("Content-Type", "application/json") // add request headers
			.addHeader("Authorization", "Bearer " + access_token)
			.post(body)
			.build();

		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful())
				throw new IOException("Unexpected code " + response + response.body().string());

			// Get response body
			json = response.body().string();

			ZenodoModel newSubmission = new Gson().fromJson(json, ZenodoModel.class);
			this.bucket = newSubmission.getLinks().getBucket();
			this.deposition_id = newSubmission.getId();

			return response.code();

		}

	}

	/**
	 * Upload files in Zenodo.
	 * @param is the inputStream for the file to upload
	 * @param file_name the name of the file as it will appear on Zenodo
	 * @param len the size of the file
	 * @return the response code
	 */
	public int uploadIS(InputStream is, String file_name, long len) throws IOException {
		OkHttpClient httpClient = new OkHttpClient
				.Builder()
				.connectTimeout(600, TimeUnit.SECONDS)
				.readTimeout(600, TimeUnit.SECONDS)
				.writeTimeout(600, TimeUnit.SECONDS).build();

		Request request = new Request.Builder()
			.url(bucket + "/" + file_name)
			.addHeader("Content-Type", "application/zip") // add request headers
			.addHeader("Authorization", "Bearer " + access_token)
			.put(InputStreamRequestBody.create(MEDIA_TYPE_ZIP, is, len))
			.build();

		try (Response response = httpClient.newCall(request).execute()) {
			if (!response.isSuccessful())
				throw new IOException("Unexpected code " + response + response.body().string());
			return response.code();
		}
	}

	/**
	 * Associates metadata information to the current deposition
	 * @param metadata the metadata
	 * @return response code
	 * @throws IOException
	 */
	public int sendMretadata(String metadata) throws IOException {

		OkHttpClient httpClient = new OkHttpClient();

		RequestBody body = RequestBody.create(MEDIA_TYPE_JSON, metadata);

		Request request = new Request.Builder()
			.url(urlString + "/" + deposition_id)
			.addHeader("Content-Type", "application/json") // add request headers
			.addHeader("Authorization", "Bearer " + access_token)
			.put(body)
			.build();

		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful())
				throw new IOException("Unexpected code " + response + response.body().string());

			return response.code();

		}

	}

	/**
	 * To publish the current deposition. It works for both new deposition or new version of an old deposition
	 * @return response code
	 * @throws IOException
	 */
	public int publish() throws IOException {

		String json = "{}";

		OkHttpClient httpClient = new OkHttpClient();

		Request request = new Request.Builder()
			.url(urlString + "/" + deposition_id + "/actions/publish")
			.addHeader("Authorization", "Bearer " + access_token)
			.post(RequestBody.create(MEDIA_TYPE_JSON, json))
			.build();

		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful())
				throw new IOException("Unexpected code " + response + response.body().string());

			return response.code();

		}
	}

	/**
	 * To create a new version of an already published deposition.
	 * It sets the deposition_id and the bucket to be used for the new version.
	 * @param concept_rec_id the concept record id of the deposition for which to create a new version. It is
	 *                       the last part of the url for the DOI Zenodo suggests to use to cite all versions:
	 *                       DOI: 10.xxx/zenodo.656930 concept_rec_id = 656930
	 * @return response code
	 * @throws IOException
	 * @throws MissingConceptDoiException
	 */
	public int newVersion(String concept_rec_id) throws IOException, MissingConceptDoiException {
		setDepositionId(concept_rec_id);
		String json = "{}";

		OkHttpClient httpClient = new OkHttpClient();

		Request request = new Request.Builder()
			.url(urlString + "/" + deposition_id + "/actions/newversion")
			.addHeader("Authorization", "Bearer " + access_token)
			.post(RequestBody.create(MEDIA_TYPE_JSON, json))
			.build();

		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful())
				throw new IOException("Unexpected code " + response + response.body().string());

			ZenodoModel zenodoModel = new Gson().fromJson(response.body().string(), ZenodoModel.class);
			String latest_draft = zenodoModel.getLinks().getLatest_draft();
			deposition_id = latest_draft.substring(latest_draft.lastIndexOf("/") + 1);
			bucket = getBucket(latest_draft);
			return response.code();

		}
	}

	private void setDepositionId(String concept_rec_id) throws IOException, MissingConceptDoiException {

		ZenodoModelList zenodoModelList = new Gson().fromJson(getPrevDepositions(), ZenodoModelList.class);

		for (ZenodoModel zm : zenodoModelList) {
			if (zm.getConceptrecid().equals(concept_rec_id)) {
				deposition_id = zm.getId();
				return;
			}
		}

		throw new MissingConceptDoiException("The concept record id specified was missing in the list of depositions");

	}

	private String getPrevDepositions() throws IOException {
		OkHttpClient httpClient = new OkHttpClient();

		Request request = new Request.Builder()
			.url(urlString)
			.addHeader("Content-Type", "application/json") // add request headers
			.addHeader("Authorization", "Bearer " + access_token)
			.get()
			.build();

		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful())
				throw new IOException("Unexpected code " + response + response.body().string());

			return response.body().string();

		}

	}

	private String getBucket(String url) throws IOException {
		OkHttpClient httpClient = new OkHttpClient();

		Request request = new Request.Builder()
			.url(url)
			.addHeader("Content-Type", "application/json") // add request headers
			.addHeader("Authorization", "Bearer " + access_token)
			.get()
			.build();

		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful())
				throw new IOException("Unexpected code " + response + response.body().string());

			// Get response body
			ZenodoModel zenodoModel = new Gson().fromJson(response.body().string(), ZenodoModel.class);

			return zenodoModel.getLinks().getBucket();

		}

	}

}
