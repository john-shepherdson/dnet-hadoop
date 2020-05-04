
package eu.dnetlib.dhp.provision.update;

import java.io.ByteArrayOutputStream;
import java.util.zip.Inflater;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.provision.scholix.ScholixResource;

public class CrossrefClient {

	private String host;
	private String index = "crossref";
	private String indexType = "item";

	public CrossrefClient(String host) {
		this.host = host;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getIndexType() {
		return indexType;
	}

	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}

	private static String decompressBlob(final String blob) {
		try {
			byte[] byteArray = Base64.decodeBase64(blob.getBytes());
			final Inflater decompresser = new Inflater();
			decompresser.setInput(byteArray);
			final ByteArrayOutputStream bos = new ByteArrayOutputStream(byteArray.length);
			byte[] buffer = new byte[8192];
			while (!decompresser.finished()) {
				int size = decompresser.inflate(buffer);
				bos.write(buffer, 0, size);
			}
			byte[] unzippeddata = bos.toByteArray();
			decompresser.end();
			return new String(unzippeddata);
		} catch (Throwable e) {
			throw new RuntimeException("Wrong record:" + blob, e);
		}
	}

	public ScholixResource getResourceByDOI(final String doi) {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(
				String
					.format(
						"http://%s:9200/%s/%s/%s", host, index, indexType, doi.replaceAll("/", "%2F")));
			CloseableHttpResponse response = client.execute(httpGet);
			String json = IOUtils.toString(response.getEntity().getContent());
			if (json.contains("blob")) {
				JsonParser p = new JsonParser();
				final JsonElement root = p.parse(json);
				json = decompressBlob(
					root.getAsJsonObject().get("_source").getAsJsonObject().get("blob").getAsString());
			}
			return CrossRefParserJSON.parseRecord(json);
		} catch (Throwable e) {
			return null;
		}
	}
}
