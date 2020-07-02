
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.OaBrokerRelatedSoftware;

public class RelatedSoftware implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 7573383356943300157L;

	private String source;
	private OaBrokerRelatedSoftware relSoftware;

	public RelatedSoftware() {
	}

	public RelatedSoftware(final String source, final OaBrokerRelatedSoftware relSoftware) {
		this.source = source;
		this.relSoftware = relSoftware;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public OaBrokerRelatedSoftware getRelSoftware() {
		return relSoftware;
	}

	public void setRelSoftware(final OaBrokerRelatedSoftware relSoftware) {
		this.relSoftware = relSoftware;
	}

}
