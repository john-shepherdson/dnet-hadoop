
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.OaBrokerProject;

public class RelatedProject implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 4941437626549329870L;

	private String source;
	private String relType;
	private OaBrokerProject relProject;

	public RelatedProject() {
	}

	public RelatedProject(final String source, final String relType, final OaBrokerProject relProject) {
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

	public OaBrokerProject getRelProject() {
		return relProject;
	}

	public void setRelProject(final OaBrokerProject relProject) {
		this.relProject = relProject;
	}

}
