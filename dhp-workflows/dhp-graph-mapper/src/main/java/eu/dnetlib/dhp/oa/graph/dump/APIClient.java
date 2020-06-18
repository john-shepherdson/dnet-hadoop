
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.*;
import java.io.IOException;

//import com.cloudera.org.apache.http.HttpResponse;
//import com.cloudera.org.apache.http.client.HttpClient;
//import com.cloudera.org.apache.http.client.methods.HttpPost;
//import com.cloudera.org.apache.http.entity.StringEntity;
//import com.cloudera.org.apache.http.impl.client.DefaultHttpClient;
//import com.cloudera.org.apache.http.util.EntityUtils;
import com.google.gson.Gson;

import eu.dnetlib.dhp.oa.graph.dump.zenodo.ZenodoModel;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;


public class APIClient implements Serializable {

	String urlString;
	String bucket;
	HttpClient client;
	String deposition_id;
	final String ACCESS_TOKEN = "5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe";

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

	public APIClient(String urlString) throws IOException {

		this.urlString = urlString;
		//connect();
	}


	public int connect() throws IOException {

		String json = "{}";


		client = new DefaultHttpClient();

		HttpPost post = new HttpPost(urlString);

		StringEntity input = new StringEntity(json);
		post.setEntity(input);
		post.addHeader("Content-Type", "application/json");
		post.setHeader("Authorization", "Bearer " + ACCESS_TOKEN);

		HttpResponse response = client.execute(post);

		json = EntityUtils.toString(response.getEntity());

		ZenodoModel newSubmission = new Gson().fromJson(json, ZenodoModel.class);
		this.bucket = newSubmission.getLinks().getBucket();
		this.deposition_id = newSubmission.getId();

		return response.getStatusLine().getStatusCode();

	}

	public void upload(String filePath, String file_name) throws IOException {
		File file = new File(filePath);
		HttpPut post = new HttpPut(bucket + "/" + file_name);
		post.setHeader("Authorization", "Bearer " + ACCESS_TOKEN);
		post.addHeader("Content-Type", "application/zip");
		HttpEntity data = MultipartEntityBuilder.create().addBinaryBody(file_name, file).build();
		post.setEntity(data);

		//HttpUriRequest request = RequestBuilder.post(bucket + "/" + file_name +"?access_token=5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe").setEntity(data).build();

		HttpResponse response = client.execute(post);
		System.out.println(response.getStatusLine().getStatusCode());

	}

}
