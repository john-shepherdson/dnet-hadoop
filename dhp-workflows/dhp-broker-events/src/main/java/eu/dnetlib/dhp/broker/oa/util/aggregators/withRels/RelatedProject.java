
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.OaBrokerProject;

public class RelatedProject implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 4941437626549329870L;

	private String source;
	private OaBrokerProject relProject;

	public RelatedProject() {
	}

	public RelatedProject(final String source, final OaBrokerProject relProject) {
		this.source = source;
		this.relProject = relProject;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public OaBrokerProject getRelProject() {
		return relProject;
	}

	public void setRelProject(final OaBrokerProject relProject) {
		this.relProject = relProject;
	}

}
