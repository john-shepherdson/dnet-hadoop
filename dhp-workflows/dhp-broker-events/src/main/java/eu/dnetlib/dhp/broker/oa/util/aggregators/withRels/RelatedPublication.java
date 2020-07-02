
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import java.io.Serializable;

import eu.dnetlib.broker.objects.OaBrokerRelatedPublication;

public class RelatedPublication implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 9021609640411395128L;

	private String source;
	private OaBrokerRelatedPublication relPublication;

	public RelatedPublication() {
	}

	public RelatedPublication(final String source, final OaBrokerRelatedPublication relPublication) {
		this.source = source;
		this.relPublication = relPublication;
	}

	public String getSource() {
		return source;
	}

	public void setSource(final String source) {
		this.source = source;
	}

	public OaBrokerRelatedPublication getRelPublication() {
		return relPublication;
	}

	public void setRelPublication(final OaBrokerRelatedPublication relPublication) {
		this.relPublication = relPublication;
	}

}
