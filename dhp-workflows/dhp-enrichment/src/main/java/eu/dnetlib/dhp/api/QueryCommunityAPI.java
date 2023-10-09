
package eu.dnetlib.dhp.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.http.HttpHeaders;
import org.jetbrains.annotations.NotNull;

import com.google.gson.Gson;

/**
 * @author miriam.baglioni
 * @Date 06/10/23
 */
public class QueryCommunityAPI {
	private static final String baseUrl = "https://services.openaire.eu/openaire/";

	private static String get(String geturl) throws IOException {
		URL url = new URL(geturl);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("GET");

		int responseCode = conn.getResponseCode();
		String body = getBody(conn);
		conn.disconnect();
		if (responseCode != HttpURLConnection.HTTP_OK)
			throw new IOException("Unexpected code " + responseCode + body);

		return body;
	}

	public static String communities() throws IOException {
		return get(baseUrl + "community/communities");
	}

	public static String community(String id) throws IOException {
		return get(baseUrl + "community/" + id);
	}

	public static String communityDatasource(String id) throws IOException {
		return get(baseUrl + "community/" + id + "/contentproviders");

	}

	public static String communityPropagationOrganization(String id) throws IOException {
		return get(baseUrl + "community/" + id + "/propagationOrganizations");
	}

	public static String communityProjects(String id, String page, String size) throws IOException {
		return get(baseUrl + "community/" + id + "/projects/" + page + "/" + size);
	}

	@NotNull
	private static String getBody(HttpURLConnection conn) throws IOException {
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

}
