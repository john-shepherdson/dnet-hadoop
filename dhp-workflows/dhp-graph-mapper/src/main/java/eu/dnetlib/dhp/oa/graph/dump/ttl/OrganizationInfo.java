
package eu.dnetlib.dhp.oa.graph.dump.ttl;

import java.io.Serializable;
import java.util.List;

public class OrganizationInfo implements Serializable {
	private String name;
	private String shortName;
	private String country;
	private String websiteUrl;
	private List<Pids> pidsList;
	private String id;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getShortName() {
		return shortName;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getWebsiteUrl() {
		return websiteUrl;
	}

	public void setWebsiteUrl(String webciteUrl) {
		this.websiteUrl = webciteUrl;
	}

	public List<Pids> getPidsList() {
		return pidsList;
	}

	public void setPidsList(List<Pids> pidsList) {
		this.pidsList = pidsList;
	}
}
