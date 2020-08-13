
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.util.List;

/**
 * To represent RC entities. It extends eu.dnetlib.dhp.dump.oaf.grap.ResearchInitiative by adding the parameter subject
 * to store the list of subjects related to the community
 */
public class ResearchCommunity extends ResearchInitiative {
	private List<String> subject;

	public List<String> getSubject() {
		return subject;
	}

	public void setSubject(List<String> subject) {
		this.subject = subject;
	}
}
