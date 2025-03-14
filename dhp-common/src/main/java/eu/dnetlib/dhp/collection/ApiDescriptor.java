
package eu.dnetlib.dhp.collection;

import java.util.HashMap;
import java.util.Map;

public class ApiDescriptor {

	private Map<String, String> params = new HashMap<>();

	private String id;

	private String baseUrl;

	private String protocol;

	private String compatibilityLevel;

	public Map<String, String> getParams() {
		return this.params;
	}

	public void setParams(final Map<String, String> params) {
		this.params = params;
	}

	public String getBaseUrl() {
		return this.baseUrl;
	}

	public void setBaseUrl(final String baseUrl) {
		this.baseUrl = baseUrl;
	}

	public String getId() {
		return this.id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getProtocol() {
		return this.protocol;
	}

	public void setProtocol(final String protocol) {
		this.protocol = protocol;
	}

	public String getCompatibilityLevel() {
		return compatibilityLevel;
	}

	public void setCompatibilityLevel(String compatibilityLevel) {
		this.compatibilityLevel = compatibilityLevel;
	}
}
