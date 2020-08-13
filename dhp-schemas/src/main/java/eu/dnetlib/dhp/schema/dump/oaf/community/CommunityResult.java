
package eu.dnetlib.dhp.schema.dump.oaf.community;

import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;
import eu.dnetlib.dhp.schema.dump.oaf.Result;

/**
 * extends eu.dnetlib.dhp.schema.dump.oaf.Result with the following parameters:
 *     - projects of type List<eu.dnetlib.dhp.schema.dump.oaf.community.Project> to store the list of projects related to the result. The information is added after the result is mapped to the external model and the description is provided afterwards
 *     - context of type List<eu.dnetlib.dhp.schema/dump.oaf.community.Context> to store information about the RC RI related to the result. For each context in the result represented in the internal model one context in the external model is produced as described above.
 *     - collectedfrom of type List<eu.dnetliv.dhp.schema.dump.oaf.KeyValue> to store information about the sources from which the record has been collected. For each collectedfrom in the result represented in the internal model one collectedfrom in the external model is produced as described for the dump of the same element within th instance.
 */
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
