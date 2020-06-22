
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;

import eu.dnetlib.dhp.oa.graph.dump.zenodo.ZenodoModel;

//import org.apache.http.entity.mime.MultipartEntityBuilder;

public class APIClient implements Serializable {

	String urlString;
	String bucket;

	String deposition_id;
	String access_token;

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

	public int connect() throws IOException {

		String json = "{}";

		CloseableHttpClient client = HttpClients.createDefault();

		HttpPost post = new HttpPost(urlString);

		StringEntity input = new StringEntity(json);
		post.setEntity(input);
		post.addHeader("Content-Type", "application/json");
		post.setHeader("Authorization", "Bearer " + access_token);

		HttpResponse response = client.execute(post);

		json = EntityUtils.toString(response.getEntity());

		ZenodoModel newSubmission = new Gson().fromJson(json, ZenodoModel.class);
		this.bucket = newSubmission.getLinks().getBucket();
		this.deposition_id = newSubmission.getId();
		client.close();
		return response.getStatusLine().getStatusCode();

	}

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

	public int upload(File file, String file_name) throws IOException {
		CloseableHttpClient client = HttpClients.createDefault();

		HttpPut put = new HttpPut(bucket + "/" + file_name);
		put.setHeader("Authorization", "Bearer " + access_token);
		put.addHeader("Content-Type", "application/zip");
		HttpEntity data = MultipartEntityBuilder.create().addBinaryBody(file_name, file).build();
		put.setEntity(data);

		HttpResponse response = client.execute(put);
		client.close();
		return response.getStatusLine().getStatusCode();

	}

	public int sendMretadata(String metadata) throws IOException {

		CloseableHttpClient client = HttpClients.createDefault();
		HttpPut post = new HttpPut(urlString + "/" + deposition_id);
		post.setHeader("Authorization", "Bearer " + access_token);
		post.addHeader("Content-Type", "application/json");
		StringEntity entity = new StringEntity(metadata, StandardCharsets.UTF_8);
		post.setEntity(entity);

		HttpResponse response = client.execute(post);
		client.close();
		return response.getStatusLine().getStatusCode();

	}

	public int publish() throws IOException {
		CloseableHttpClient client = HttpClients.createDefault();
		HttpPost post = new HttpPost(urlString + "/" + deposition_id + "/actions/publish");
		post.setHeader("Authorization", "Bearer " + access_token);

		HttpResponse response = client.execute(post);
		client.close();
		return response.getStatusLine().getStatusCode();
	}

}
