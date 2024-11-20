
package eu.dnetlib.dhp.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import org.jetbrains.annotations.NotNull;

/**
 * @author miriam.baglioni
 * @Date 06/10/23
 */
public class QueryCommunityAPI {

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

	public static String communities(String baseURL) throws IOException {

		return get(baseURL + "communities");

	}

	public static String subcommunities(String communityId, String baseURL) throws IOException {

		return get(baseURL + communityId + "/subcommunities");

	}


	public static String communityDatasource(String id, String baseURL) throws IOException {

		return get(baseURL + id + "/datasources");

	}



	public static String communityPropagationOrganization(String id, String baseURL) throws IOException {

		return get(baseURL + id + "/propagationOrganizations");

	}

	public static String communityProjects(String id, String page, String size, String baseURL) throws IOException {

		return get(baseURL + id + "/projects/" + page + "/" + size);

	}

	public static String propagationOrganizationCommunityMap(String baseURL) throws IOException {
		return get(baseURL + "/propagationOrganizationCommunityMap");
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

	public static String subcommunityDatasource(String communityId, String subcommunityId, String baseURL) throws IOException {
		return get(baseURL + communityId + "/subcommunities/datasources?subCommunityId=" + subcommunityId);
	}

	public static String subcommunityPropagationOrganization(String communityId, String subcommunityId , String baseURL) throws IOException {
		return get(baseURL + communityId + "/subcommunities/propagationOrganizations?subCommunityId=" + subcommunityId);
	}

	public static String subcommunityProjects(String communityId, String subcommunityId, String page, String size, String baseURL) throws IOException {
		return get(baseURL + communityId + "/subcommunities/projects/" + page + "/" + size + "?subCommunityId=" + subcommunityId);
	}

	public static String propagationDatasourceCommunityMap(String baseURL) throws IOException {
		return get(baseURL + "/propagationDatasourceCommunityMap");
	}
}
