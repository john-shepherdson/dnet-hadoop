
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.*;
import java.io.IOException;
import okhttp3.*;
import com.google.gson.Gson;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.ZenodoModel;


public class APIClient implements Serializable {

	String urlString;
	String bucket;

	String deposition_id;
	String access_token;

	public static final MediaType MEDIA_TYPE_JSON
			= MediaType.parse("application/json; charset=utf-8");

	private static final MediaType MEDIA_TYPE_ZIP
			= MediaType.parse("application/zip");

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

	public APIClient(String urlString, String access_token) throws IOException {

		this.urlString = urlString;
		this.access_token = access_token;
	}

	public int connect() throws IOException{
		String json = "{}";
		OkHttpClient httpClient = new OkHttpClient();

		RequestBody body = RequestBody.create(json, MEDIA_TYPE_JSON);

		Request request = new Request.Builder()
				.url(urlString)
				.addHeader("Content-Type", "application/json")  // add request headers
				.addHeader("Authorization", "Bearer " + access_token)
				.post(body)
				.build();



		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful()) throw new IOException("Unexpected code " + response + response.body().string());

			// Get response body
			json = response.body().string();

			ZenodoModel newSubmission = new Gson().fromJson(json, ZenodoModel.class);
			this.bucket = newSubmission.getLinks().getBucket();
			this.deposition_id = newSubmission.getId();

			return response.code();

		}

	}


	public int upload(File file, String file_name){

		OkHttpClient httpClient = new OkHttpClient();

		Request request = new Request.Builder()
				.url(bucket + "/" + file_name)
				.addHeader("Content-Type", "application/zip")  // add request headers
				.addHeader("Authorization", "Bearer " + access_token)
				.put(RequestBody.create(file, MEDIA_TYPE_ZIP))
				.build();

		try (Response response = httpClient.newCall(request).execute()) {
			if (!response.isSuccessful()) throw new IOException("Unexpected code " + response + response.body().string());
			return response.code();
		} catch (IOException e) {
			e.printStackTrace();

		}

		return -1;
	}


	public int sendMretadata(String metadata) throws IOException {

		OkHttpClient httpClient = new OkHttpClient();

		RequestBody body = RequestBody.create(metadata, MEDIA_TYPE_JSON);

		Request request = new Request.Builder()
				.url(urlString + "/" + deposition_id)
				.addHeader("Content-Type", "application/json")  // add request headers
				.addHeader("Authorization", "Bearer " + access_token)
				.put(body)
				.build();


		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful()) throw new IOException("Unexpected code " + response + response.body().string());

			return response.code();

		}

	}

	public int publish() throws IOException {

		String json = "{}";

		OkHttpClient httpClient = new OkHttpClient();


		Request request = new Request.Builder()
				.url(urlString + "/" + deposition_id + "/actions/publish")
				.addHeader("Authorization", "Bearer " + access_token)
				.post(RequestBody.create(json, MEDIA_TYPE_JSON))
				.build();

		try (Response response = httpClient.newCall(request).execute()) {

			if (!response.isSuccessful()) throw new IOException("Unexpected code " + response + response.body().string());

			return response.code();

		}
	}

	//	public int connect() throws IOException {
//
//		String json = "{}";
//
//		HttpClient client = new DefaultHttpClient();
//
//		HttpPost post = new HttpPost(urlString);
//
//		StringEntity input = new StringEntity(json);
//		post.setEntity(input);
//		post.addHeader("Content-Type", "application/json");
//		post.setHeader("Authorization", "Bearer " + access_token);
//
//
//		HttpResponse response = client.execute(post);
//
//		json = EntityUtils.toString(response.getEntity());
//
//		ZenodoModel newSubmission = new Gson().fromJson(json, ZenodoModel.class);
//		this.bucket = newSubmission.getLinks().getBucket();
//		this.deposition_id = newSubmission.getId();
//
//		return response.getStatusLine().getStatusCode();
//
//	}

//	public int upload(InputStream is, String file_name) throws IOException {
//		HttpClient client = new DefaultHttpClient();
//
//		HttpPut put = new HttpPut(bucket + "/" + file_name);
//		put.setHeader("Authorization", "Bearer " + access_token);
//		put.addHeader("Content-Type", "application/zip");
//
//		HttpEntity data = MultipartEntityBuilder
//			.create()
//			// .addPart("file", new ByteArrayInputStream(is));
//			.addBinaryBody(file_name, is, ContentType.APPLICATION_OCTET_STREAM, file_name)
//			.build();
//		put.setEntity(data);
//
//		HttpResponse response = client.execute(put);
//
//		return response.getStatusLine().getStatusCode();
//	}

//	public int upload(File file, String file_name) throws IOException {
//		HttpClient client = new DefaultHttpClient();
//
//		HttpPut put = new HttpPut(bucket + "/" + file_name);
//		put.setHeader("Authorization", "Bearer " + access_token);
//		put.addHeader("Content-Type", "application/zip");
//		InputStreamEntity data = new InputStreamEntity(new FileInputStream(file), -1);
//		data.setContentType("binary/octet-stream");
//		data.setChunked(true); // Send in multiple parts if needed
////		ByteArrayInputStream bais = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
////		EntityUtils.toByteArray(new ByteArrayInputStream(FileUtils.readFileToByteArray(file)));
////		InputStream targetStream = new FileInputStream(file);
////		DataInputStream tmp = new DataInputStream(targetStream);
////		HttpEntity data = new ByteArrayEntity(tmp.);
////		HttpEntity data = MultipartEntityBuilder.create().addBinaryBody(file_name, file).build();
//		put.setEntity(data);
//
//		HttpResponse response = client.execute(put);
//
//		return response.getStatusLine().getStatusCode();
//
//	}

//	public int sendMretadata(String metadata) throws IOException {
//
//		HttpClient client = new DefaultHttpClient();
//		HttpPut post = new HttpPut(urlString + "/" + deposition_id);
//		post.setHeader("Authorization", "Bearer " + access_token);
//		post.addHeader("Content-Type", "application/json");
//		StringEntity entity = new StringEntity(metadata, StandardCharsets.UTF_8);
//		post.setEntity(entity);
//
//		HttpResponse response = client.execute(post);
//
//		return response.getStatusLine().getStatusCode();
//
//	}

//	public int publish() throws IOException {
//		HttpClient client = new DefaultHttpClient();
//		HttpPost post = new HttpPost(urlString + "/" + deposition_id + "/actions/publish");
//		post.setHeader("Authorization", "Bearer " + access_token);
//
//		HttpResponse response = client.execute(post);
//
//		return response.getStatusLine().getStatusCode();
//	}

}
