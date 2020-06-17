
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.Project;

public class RelatedProject implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 4941437626549329870L;

	private final String source;
	private final String relType;
	private final Project relProject;

	public RelatedProject(final String source, final String relType, final Project relProject) {
		this.source = source;
		this.relType = relType;
		this.relProject = relProject;
	}

	public String getSource() {
		return source;
	}

	public String getRelType() {
		return relType;
	}

	public Project getRelProject() {
		return relProject;
	}

}
