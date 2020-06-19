
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.Project;

public class RelatedProject implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 4941437626549329870L;

	private String source;
	private String relType;
	private Project relProject;

	public RelatedProject() {
	}

	public RelatedProject(final String source, final String relType, final Project relProject) {
		this.source = source;
		this.relType = relType;
		this.relProject = relProject;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public String getRelType() {
		return relType;
	}

	public void setRelType(final String relType) {
		this.relType = relType;
	}

	public Project getRelProject() {
		return relProject;
	}

	public void setRelProject(final Project relProject) {
		this.relProject = relProject;
	}

}
