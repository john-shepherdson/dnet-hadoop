
package eu.dnetlib.dhp.schema.dump.oaf.community;

import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;
import eu.dnetlib.dhp.schema.dump.oaf.Result;

public class CommunityResult extends Result {

	private List<Project> projects;

	private List<Context> context;

	protected List<KeyValue> collectedfrom;

	public List<KeyValue> getCollectedfrom() {
		return collectedfrom;
	}

	public void setCollectedfrom(List<KeyValue> collectedfrom) {
		this.collectedfrom = collectedfrom;
	}

	public List<Project> getProjects() {
		return projects;
	}

	public void setProjects(List<Project> projects) {
		this.projects = projects;
	}

	public List<Context> getContext() {
		return context;
	}

	public void setContext(List<Context> context) {
		this.context = context;
	}
}
