
package eu.dnetlib.dhp.bulktag.community;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

/** Created by miriam on 01/08/2018. */
public class Community implements Serializable {

	private String id;
	private List<String> subjects = new ArrayList<>();
	private List<Provider> providers = new ArrayList<>();
	private List<ZenodoCommunity> zenodoCommunities = new ArrayList<>();
	private SelectionConstraints constraints = new SelectionConstraints();

	public String toJson() {
		final Gson g = new Gson();
		return g.toJson(this);
	}

	public boolean isValid() {
		return !getSubjects().isEmpty()
			|| !getProviders().isEmpty()
			|| !getZenodoCommunities().isEmpty()
			|| getConstraints().getCriteria() != null;
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

	public SelectionConstraints getConstraints() {
		return constraints;
	}

	public void setConstraints(SelectionConstraints constraints) {
		this.constraints = constraints;
	}
}
