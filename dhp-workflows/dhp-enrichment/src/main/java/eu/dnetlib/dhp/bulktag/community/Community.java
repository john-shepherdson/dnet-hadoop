
package eu.dnetlib.dhp.bulktag.community;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;

/** Created by miriam on 01/08/2018. */
public class Community implements Serializable {

	private static final Log log = LogFactory.getLog(Community.class);

	private String id;
	private List<String> subjects = new ArrayList<>();
	private List<Provider> providers = new ArrayList<>();
	private List<ZenodoCommunity> zenodoCommunities = new ArrayList<>();

	public String toJson() {
		final Gson g = new Gson();
		return g.toJson(this);
	}

	public boolean isValid() {
		return !getSubjects().isEmpty()
			|| !getProviders().isEmpty()
			|| !getZenodoCommunities().isEmpty();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getSubjects() {
		return subjects;
	}

	public void setSubjects(List<String> subjects) {
		this.subjects = subjects;
	}

	public List<Provider> getProviders() {
		return providers;
	}

	public void setProviders(List<Provider> providers) {
		this.providers = providers;
	}

	public List<ZenodoCommunity> getZenodoCommunities() {
		return zenodoCommunities;
	}

	public void setZenodoCommunities(List<ZenodoCommunity> zenodoCommunities) {
		this.zenodoCommunities = zenodoCommunities;
	}
}
